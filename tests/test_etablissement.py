"""Tests du job Silver pipelines/silver/etablissement.py : Spark local, dossier temporaire, données synthétiques.

    python -m unittest tests.test_etablissement

Se lancent dans Docker (voir CLAUDE.md §13).
TestExecuter vérifie la plomberie du job avec un transformer factice : elle ne dépend pas des règles métier.
TestTransformer vérifie les règles métier ; TestBoutEnBout fait tourner le job avec le vrai transformer.
"""

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest import mock

from pyspark.sql import functions as F

from pipelines.bronze.csv_bronze import CATALOGUE
from pipelines.common.delta_io import ajouter_delta
from pipelines.common.normalize import normaliser_identifiant_organisation
from pipelines.common.session_spark import chemin_lake, creer_session
from pipelines.silver import etablissement

ETAB = next(c for c in CATALOGUE if c.table == "s2_etablissement_sante")
SCHEMA_BRONZE = (
    ", ".join(f"{c} string" for c in ETAB.colonnes)
    + ", _ingestion_ts timestamp, _source string, _batch_id string, _fichier_origine string"
)


def ts(jour):
    return datetime(2026, 10, jour)


def ligne_bronze(identifiant, raison="Cabinet synthétique", lot="b1", jour=1, **autres):
    """Une ligne Bronze synthétique ; `autres` remplace ou ajoute des valeurs de colonnes."""
    valeurs = {c: None for c in ETAB.colonnes}
    valeurs.update(
        identifiant_organisation=identifiant, raison_sociale_site=raison, code_postal="75008",
        adresse="1 rue Synthétique", email="contact@exemple.invalid", telephone="0100000000",
    )
    valeurs.update(autres)
    return tuple(valeurs[c] for c in ETAB.colonnes) + (ts(jour), "s2", lot, "etablissement_sante.csv")


def transformer_factice(bronze):
    """Règle minimale : identifiant normalisé obligatoire, le reste est constant."""
    identifiant = normaliser_identifiant_organisation(F.col("identifiant_organisation"))
    rejetees = bronze.filter(identifiant.isNull()).withColumn("motif", F.lit("identifiant_manquant"))
    valides = bronze.filter(identifiant.isNotNull()).select(
        identifiant.alias("identifiant_organisation"),
        F.lit(None).cast("string").alias("finess_site"),
        F.lit(None).cast("string").alias("finess_juridique"),
        F.col("raison_sociale_site").alias("raison_sociale"),
        F.lit("autre").alias("type_etablissement"),
        F.col("commune"),
        F.col("code_commune"),
        F.col("code_postal"),
        F.lit("75").alias("departement"),
        F.lit("11").alias("code_region"),
        F.lit("Île-de-France").alias("libelle_region"),
        "_ingestion_ts", "_source", "_batch_id", "_fichier_origine",
    )
    return valides, rejetees


class AvecLacBronze(unittest.TestCase):
    """Session Spark du projet et lac temporaire où l'on dépose des lots Bronze synthétiques."""

    @classmethod
    def setUpClass(cls):
        with mock.patch.dict(os.environ, {"LAKE_ROOT": tempfile.gettempdir()}):
            cls.spark = creer_session("test_etablissement")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.lac = tempfile.mkdtemp()
        self.env = mock.patch.dict(os.environ, {"LAKE_ROOT": self.lac})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.lac, ignore_errors=True)

    def bronze(self, lignes):
        df = self.spark.createDataFrame(lignes, SCHEMA_BRONZE)
        ajouter_delta(self.spark, df, chemin_lake("bronze", "s2", "etablissement_sante"))

    def lire(self, *chemin):
        return self.spark.read.format("delta").load(chemin_lake(*chemin))

    def journal(self):
        return self.lire("silver", "_journal").orderBy("debut").collect()


class TestExecuter(AvecLacBronze):
    def setUp(self):
        super().setUp()
        self.transformer = mock.patch.object(etablissement, "transformer", transformer_factice)
        self.transformer.start()

    def tearDown(self):
        self.transformer.stop()
        super().tearDown()

    def lot_type(self):
        # F1 et « f 1 » sont la même clé après normalisation ; deux identifiants vides à rejeter
        self.bronze([
            ligne_bronze("F1", "Premier"), ligne_bronze("f 1", "Doublon"), ligne_bronze("F2"),
            ligne_bronze(None), ligne_bronze("   "),
        ])

    def test_premiere_execution_ecrit_silver_rejets_et_journal(self):
        self.lot_type()
        self.assertEqual(etablissement.executer(self.spark), "ok")

        silver = self.lire("silver", "etablissement")
        self.assertEqual(sorted(r.identifiant_organisation for r in silver.collect()), ["F1", "F2"])
        self.assertEqual(silver.columns, list(etablissement.COLONNES_SILVER))

        rejets = self.lire("silver", "_rejets", "s2").collect()
        self.assertEqual([r.motif for r in rejets], ["identifiant_manquant"] * 2)
        contenu = json.loads(rejets[0].ligne_origine)
        self.assertEqual(contenu["code_postal"], "75008")
        for interdit in ("adresse", "email", "telephone"):
            self.assertNotIn(interdit, contenu)

        j = self.journal()[0]
        self.assertEqual((j.job, j.source, j.table, j.statut, j.batch_id), ("silver_etablissement", "s2", "etablissement", "ok", "b1"))
        self.assertEqual((j.lignes_lues, j.lignes_ecrites, j.lignes_rejetees, j.lignes_dedoublonnees), (5, 2, 2, 1))

    def test_relancer_sur_le_meme_lot_ne_change_rien(self):
        self.lot_type()
        etablissement.executer(self.spark)
        etablissement.executer(self.spark)
        self.assertEqual(self.lire("silver", "etablissement").count(), 2)
        self.assertEqual(self.lire("silver", "_rejets", "s2").count(), 2)
        j1, j2 = self.journal()
        self.assertEqual((j2.lignes_lues, j2.lignes_ecrites, j2.lignes_rejetees, j2.lignes_dedoublonnees), (5, 0, 2, 1))

    def test_nouveau_lot_met_a_jour_sans_doublon_et_ne_lit_que_le_dernier_lot(self):
        self.bronze([ligne_bronze("F1", "Ancien nom", "b1", 1), ligne_bronze("F2", "Inchangé", "b1", 1)])
        etablissement.executer(self.spark)
        self.bronze([ligne_bronze("F1", "Nouveau nom", "b2", 5), ligne_bronze("F3", "Nouveau", "b2", 5)])
        etablissement.executer(self.spark)

        noms = {r.identifiant_organisation: r.raison_sociale for r in self.lire("silver", "etablissement").collect()}
        self.assertEqual(noms, {"F1": "Nouveau nom", "F2": "Inchangé", "F3": "Nouveau"})
        j = self.journal()[1]
        self.assertEqual((j.batch_id, j.lignes_lues, j.lignes_ecrites, j.lignes_dedoublonnees), ("b2", 2, 2, 0))

    def test_echec_de_transformer_est_journalise_et_propage(self):
        self.lot_type()
        with mock.patch.object(etablissement, "transformer", side_effect=NotImplementedError("pas écrit")):
            with self.assertRaises(NotImplementedError):
                etablissement.executer(self.spark)
        j = self.journal()[0]
        self.assertEqual((j.statut, j.message, j.lignes_lues), ("echec", "pas écrit", 5))

    def test_transformer_qui_perd_des_lignes_est_refuse(self):
        self.lot_type()

        def perd_tout(bronze):
            valides, rejetees = transformer_factice(bronze)
            return valides, rejetees.limit(1)  # un rejet disparaît

        with mock.patch.object(etablissement, "transformer", perd_tout):
            with self.assertRaisesRegex(RuntimeError, "perdu ou dupliqué"):
                etablissement.executer(self.spark)
        self.assertFalse(os.path.exists(chemin_lake("silver", "etablissement")))

    def test_transformer_avec_de_mauvaises_colonnes_est_refuse(self):
        self.lot_type()

        def mauvaises_colonnes(bronze):
            valides, rejetees = transformer_factice(bronze)
            return valides.drop("departement"), rejetees

        with mock.patch.object(etablissement, "transformer", mauvaises_colonnes):
            with self.assertRaisesRegex(ValueError, "colonnes"):
                etablissement.executer(self.spark)


class TestTransformer(AvecLacBronze):
    """Règles métier de transformer(), sur des lignes Bronze synthétiques identifiées par leur identifiant."""

    def transformer(self, *lignes):
        bronze = self.spark.createDataFrame(list(lignes), SCHEMA_BRONZE)
        valides, rejetees = etablissement.transformer(bronze)
        return bronze, valides, rejetees

    def valides_par_id(self, *lignes):
        _, valides, rejetees = self.transformer(*lignes)
        self.assertEqual(rejetees.count(), 0)
        return {r.identifiant_organisation: r for r in valides.collect()}

    def test_colonnes_dans_l_ordre_sans_donnee_de_contact(self):
        _, valides, _ = self.transformer(ligne_bronze("F1"))
        self.assertEqual(valides.columns, list(etablissement.COLONNES_SILVER))
        for interdit in ("adresse", "email", "telephone", "telecopie", "voie", "libelle_raison"):
            self.assertNotIn(interdit, valides.columns)

    def test_colonnes_techniques_conservees(self):
        ligne = self.valides_par_id(ligne_bronze("F1", lot="b7", jour=3))["F1"]
        self.assertEqual((ligne._ingestion_ts, ligne._source, ligne._batch_id, ligne._fichier_origine),
                         (ts(3), "s2", "b7", "etablissement_sante.csv"))

    def test_identifiant_normalise(self):
        par_id = self.valides_par_id(ligne_bronze(" f750 712184 "), ligne_bronze("R00001"))
        self.assertEqual(sorted(par_id), ["F750712184", "R00001"])

    def test_codes_complets_a_gauche_et_nuls_si_invalides(self):
        ligne = self.valides_par_id(ligne_bronze(
            "F1", finess_site="12345678", finess_etablissement_juridique="2A0000012",
            code_commune="1001", code_postal="1000",
        ))["F1"]
        self.assertEqual(
            (ligne.finess_site, ligne.finess_juridique, ligne.code_commune, ligne.code_postal),
            ("012345678", "2A0000012", "01001", "01000"),
        )
        ligne = self.valides_par_id(ligne_bronze(
            "F1", finess_site="", finess_etablissement_juridique="1234567890", code_commune="ABCDE", code_postal="75",
        ))["F1"]
        self.assertEqual((ligne.finess_site, ligne.finess_juridique, ligne.code_commune, ligne.code_postal),
                         (None, None, None, None))

    def test_geographie(self):
        cas = {  # identifiant: (code_commune, code_postal, département, code région, libellé région)
            "A": ("1001", "01000", "01", "84", "Auvergne-Rhône-Alpes"),
            "B": (None, "20137", "2A", "94", "Corse"),
            "C": ("97105", None, "971", "01", "Guadeloupe"),
            "D": ("75056", "13001", "75", "11", "Île-de-France"),  # la commune prime sur le code postal
            "E": ("ABCDE", "XY", "NR", "NR", "Non renseigné"),
            "F": (None, None, "NR", "NR", "Non renseigné"),
        }
        par_id = self.valides_par_id(*[
            ligne_bronze(i, code_commune=c[0], code_postal=c[1]) for i, c in cas.items()
        ])
        for identifiant, (_, _, dep, region, libelle) in cas.items():
            with self.subTest(identifiant=identifiant):
                ligne = par_id[identifiant]
                self.assertEqual((ligne.departement, ligne.code_region, ligne.libelle_region), (dep, region, libelle))

    def test_libelles_en_majuscules_sans_accents_ou_non_renseignes(self):
        par_id = self.valides_par_id(
            ligne_bronze("A", "  Clinique de l'Étoile ", commune="Évry"),
            ligne_bronze("B", None, commune=None),
            ligne_bronze("C", "   ", commune=""),
        )
        a, b, c = par_id["A"], par_id["B"], par_id["C"]
        self.assertEqual((a.raison_sociale, a.commune), ("CLINIQUE DE L'ETOILE", "EVRY"))
        self.assertEqual((b.raison_sociale, b.commune), ("Non renseigné", "Non renseigné"))
        self.assertEqual((c.raison_sociale, c.commune), ("Non renseigné", "Non renseigné"))

    def test_type_etablissement(self):
        cas = {
            "CABINET DU DOCTEUR MARTIN": "Cabinet libéral",
            "SELARL Martin": "Cabinet libéral",
            "Pharmacie du Centre": "Pharmacie",
            "Cabinet de radiologie": "Imagerie",  # le plus spécifique d'abord
            "Scanner de l'Ouest": "Imagerie",
            "Centre hospitalier d'Évry": "Centre hospitalier",
            "CH de Saint-Lô": "Centre hospitalier",
            "Centre Hospitalier Universitaire de Lille": "CHU / régional",
            "CHRU de Tours": "CHU / régional",
            "Clinique Saint-Joseph": "Clinique",
            "LBM Biogroup": "Laboratoire",
            "CDS de Lyon": "Centre de santé",
            "Centre de santé municipal": "Centre de santé",
            "EHPAD Les Lilas": "Médico-social",
            "SSIAD du Val": "Médico-social",
            "MAS des Pins": "Médico-social",
            "Optique Dupont": "Autre",
            "CHARLES MARTIN": "Autre",  # CH n'est reconnu que comme mot entier
            "Cabinet CH Dupont": "Cabinet libéral",  # sigles courts ancrés en début de libellé
            "Cabinet MAS": "Cabinet libéral",
        }
        identifiants = {raison: f"F{i}" for i, raison in enumerate(cas)}
        par_id = self.valides_par_id(*[ligne_bronze(identifiants[r], r) for r in cas])
        for raison, attendu in cas.items():
            with self.subTest(raison=raison):
                self.assertEqual(par_id[identifiants[raison]].type_etablissement, attendu)

    def test_raison_sociale_absente_donne_le_type_autre(self):
        self.assertEqual(self.valides_par_id(ligne_bronze("F1", None))["F1"].type_etablissement, "Autre")

    def test_identifiant_manquant_est_rejete_avec_ses_colonnes_bronze(self):
        bronze, valides, rejetees = self.transformer(
            ligne_bronze("F1"), ligne_bronze(None), ligne_bronze(""), ligne_bronze("   ")
        )
        self.assertEqual(valides.count(), 1)
        self.assertEqual(rejetees.count(), 3)
        self.assertEqual(rejetees.columns, bronze.columns + ["motif"])
        self.assertEqual({r.motif for r in rejetees.collect()}, {"identifiant_organisation_manquant"})
        self.assertEqual(valides.count() + rejetees.count(), bronze.count())  # aucune ligne ne disparaît


class TestBoutEnBout(AvecLacBronze):
    """Le job complet avec le vrai transformer."""

    def test_job_complet(self):
        self.bronze([
            ligne_bronze("F1", "Clinique du Parc", code_commune="75056", code_postal="75008", commune="Paris"),
            ligne_bronze("f1", "Clinique du Parc (doublon)", code_commune="75056", code_postal="75008", commune="Paris"),
            ligne_bronze("F2", "Cabinet du Dr Martin", code_commune=None, code_postal="20137"),
            ligne_bronze(None),
        ])
        self.assertEqual(etablissement.executer(self.spark), "ok")

        silver = {r.identifiant_organisation: r for r in self.lire("silver", "etablissement").collect()}
        self.assertEqual(sorted(silver), ["F1", "F2"])
        self.assertEqual((silver["F1"].type_etablissement, silver["F1"].departement), ("Clinique", "75"))
        self.assertEqual((silver["F2"].type_etablissement, silver["F2"].departement, silver["F2"].commune),
                         ("Cabinet libéral", "2A", "Non renseigné"))

        j = self.journal()[0]
        self.assertEqual((j.lignes_lues, j.lignes_ecrites, j.lignes_rejetees, j.lignes_dedoublonnees), (4, 2, 1, 1))
        self.assertEqual(self.lire("silver", "_rejets", "s2").first().motif, "identifiant_organisation_manquant")


if __name__ == "__main__":
    unittest.main()
