"""Tests de l'ingestion CSV vers Bronze : Spark en local, dossier temporaire, données synthétiques.

    python -m unittest tests.test_csv_bronze
"""

import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pyspark.sql import functions as F

from pipelines.bronze import csv_bronze
from pipelines.bronze.csv_bronze import COLONNES_TECHNIQUES, chemin_lake

DDL = Path(__file__).resolve().parent.parent / "pipelines" / "bronze" / "CSV_schema.sql"
ACTIVITE = next(c for c in csv_bronze.CATALOGUE if c.table == "s2_activite_professionnel_sante")
PROFESSIONNEL = next(c for c in csv_bronze.CATALOGUE if c.table == "s2_professionnel_sante")
ETABLISSEMENT = next(c for c in csv_bronze.CATALOGUE if c.table == "s2_etablissement_sante")
HOSPITALISATION = next(c for c in csv_bronze.CATALOGUE if c.table == "hosp_hospitalisations")


class TestCsvBronze(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # session locale : sans LAKE_ROOT, le défaut s3a:// exigerait la configuration MinIO
        with mock.patch.dict(os.environ, {"LAKE_ROOT": tempfile.gettempdir()}):
            cls.spark = csv_bronze.creer_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.lac = tempfile.mkdtemp()
        self.entree = Path(tempfile.mkdtemp()) / "Etablissement de SANTE"
        self.entree.mkdir()
        self.env = mock.patch.dict(os.environ, {"LAKE_ROOT": self.lac})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.lac, ignore_errors=True)
        shutil.rmtree(self.entree.parent, ignore_errors=True)

    def ecrire(self, nom, contenu):
        chemin = self.entree / nom
        chemin.write_bytes(contenu if isinstance(contenu, bytes) else contenu.encode("utf-8"))
        return chemin

    def table(self, cfg):
        return self.spark.read.format("delta").load(cfg.chemin_delta)

    def journal(self):
        return self.spark.read.format("delta").load(chemin_lake("bronze", "_journal"))

    def test_colonnes_alignees_sur_le_ddl(self):
        sql = DDL.read_text(encoding="utf-8")
        for cfg in csv_bronze.CATALOGUE:
            corps = re.search(rf"bronze\.{cfg.table} \((.*?)\)\s*USING DELTA\s*LOCATION '([^']+)'", sql, re.S)
            colonnes = [ligne.split()[0] for ligne in corps.group(1).split(",\n") if ligne.strip()]
            self.assertEqual(colonnes, list(cfg.colonnes) + list(COLONNES_TECHNIQUES), cfg.table)
            with mock.patch.dict(os.environ, {"LAKE_ROOT": ""}):
                self.assertEqual(corps.group(2), cfg.chemin_delta, cfg.table)

    def test_colonnes_techniques_et_batch_id_stable(self):
        fichier = self.ecrire("activite_professionnel_sante.csv", "identifiant;identifiant_organisation;mode_exercice\n1;A;S\n2;B;L\n")
        csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier)
        df = self.table(ACTIVITE)
        self.assertEqual(df.columns, list(ACTIVITE.colonnes) + list(COLONNES_TECHNIQUES))
        self.assertEqual(df.count(), 2)
        self.assertEqual(df.filter(F.col("_ingestion_ts").isNull()).count(), 0)
        ligne = df.first()
        self.assertEqual(ligne["_source"], "s2")
        self.assertEqual(ligne["_fichier_origine"], "activite_professionnel_sante.csv")
        self.assertEqual(df.select("_batch_id").distinct().count(), 1)

    def test_relancer_ne_cree_pas_de_doublon(self):
        fichier = self.ecrire("activite_professionnel_sante.csv", "identifiant;identifiant_organisation;mode_exercice\n1;A;S\n2;B;L\n")
        self.assertEqual(csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier), "ok")
        self.assertEqual(csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier), "ignore")
        self.assertEqual(self.table(ACTIVITE).count(), 2)
        self.assertEqual(sorted(r["statut"] for r in self.journal().collect()), ["ignore", "ok"])

    def test_nouvelle_version_s_ajoute(self):
        fichier = self.ecrire("activite_professionnel_sante.csv", "identifiant;identifiant_organisation;mode_exercice\n1;A;S\n")
        csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier)
        fichier = self.ecrire("activite_professionnel_sante.csv", "identifiant;identifiant_organisation;mode_exercice\n1;A;S\n2;B;L\n")
        csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier)
        df = self.table(ACTIVITE)
        self.assertEqual(df.count(), 3)
        self.assertEqual(df.select("_batch_id").distinct().count(), 2)

    def test_separateur_dans_un_champ_entre_guillemets(self):
        entete = ";".join(ETABLISSEMENT.colonnes)
        champs = [f"valeur{i}" for i in range(len(ETABLISSEMENT.colonnes))]
        champs[0] = '"12 rue A; bis"'
        fichier = self.ecrire("etablissement_sante.csv", entete + "\n" + ";".join(champs) + "\n")
        csv_bronze.ingerer_csv(self.spark, ETABLISSEMENT, fichier)
        ligne = self.table(ETABLISSEMENT).first()
        self.assertEqual(ligne["adresse"], "12 rue A; bis")
        self.assertEqual(ligne["cedex"], "valeur1")
        self.assertEqual(ligne["voie"], "valeur23")

    def test_encodage_mixte_lu_sans_perte(self):
        entete = ";".join(PROFESSIONNEL.colonnes).encode()
        utf8 = "1;Mme;X;Dupont;Hélène;Paris;P;S;ADELI".encode("utf-8")
        cp1252 = "2;Mme;X;Durand;Hélène;Lyon;P;S;ADELI".encode("cp1252")
        fichier = self.ecrire("professionnel_sante.csv", entete + b"\n" + utf8 + b"\n" + cp1252 + b"\n")
        csv_bronze.ingerer_csv(self.spark, PROFESSIONNEL, fichier)
        prenoms = {r["identifiant"]: r["prenom"] for r in self.table(PROFESSIONNEL).collect()}
        self.assertEqual(prenoms["2"], "Hélène")
        self.assertEqual(prenoms["1"].encode("latin-1").decode("utf-8"), "Hélène")

    def test_entete_inattendu_echoue(self):
        fichier = self.ecrire("activite_professionnel_sante.csv", "id;org;mode\n1;A;S\n")
        with self.assertRaisesRegex(ValueError, "En-tête inattendu"):
            csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier)
        self.assertEqual(self.journal().first()["statut"], "echec")

    def test_entetes_en_casse_mixte(self):
        entete = "Num_Hospitalisation;Id_patient;identifiant_organisation;Code_diagnostic;Suite_diagnostic_consultation;Date_Entree;Jour_Hospitalisation"
        fichier = self.ecrire("Hospitalisations.csv", entete + "\n1;P1;O1;D1;S1;2020-01-01;3\n")
        csv_bronze.ingerer_csv(self.spark, HOSPITALISATION, fichier)
        ligne = self.table(HOSPITALISATION).first()
        self.assertEqual((ligne["num_hospitalisation"], ligne["date_entree"], ligne["_source"]), ("1", "2020-01-01", "hosp"))

    def test_ligne_malformee_va_en_rejets_et_le_journal_est_coherent(self):
        entete = "identifiant;identifiant_organisation;mode_exercice"
        fichier = self.ecrire("activite_professionnel_sante.csv", f"{entete}\n1;A;S\n2;B\n3;C;S;en trop\n4;D;L\n")
        csv_bronze.ingerer_csv(self.spark, ACTIVITE, fichier)
        self.assertEqual(self.table(ACTIVITE).count(), 2)
        rejets = self.spark.read.format("delta").load(chemin_lake("silver", "_rejets", "s2"))
        self.assertEqual(rejets.count(), 2)
        self.assertEqual(rejets.first()["motif"], "ligne_malformee")
        j = self.journal().first()
        self.assertEqual((j["lignes_lues"], j["lignes_ecrites"], j["lignes_rejetees"]), (4, 2, 2))
        self.assertEqual(j["lignes_lues"], j["lignes_ecrites"] + j["lignes_rejetees"])


if __name__ == "__main__":
    unittest.main()
