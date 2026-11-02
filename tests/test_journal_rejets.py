"""Tests de pipelines/common/journal.py et rejets.py : Spark local, dossier temporaire, données synthétiques.

    python -m unittest tests.test_journal_rejets

Se lancent dans Docker (voir CLAUDE.md §13) : les deux modules écrivent des tables Delta.
"""

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

from pipelines.common import journal, rejets
from pipelines.common.delta_io import ajouter_delta
from pipelines.common.session_spark import chemin_lake, creer_session

DEBUT = datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc)
SCHEMA_REJETEES = "motif string, id string, adresse string, code_postal string, _batch_id string, _fichier_origine string"


class AvecLac(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with mock.patch.dict(os.environ, {"LAKE_ROOT": tempfile.gettempdir()}):
            cls.spark = creer_session("test_journal_rejets")

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


class TestJournal(AvecLac):
    def lire(self, couche):
        return self.spark.read.format("delta").load(chemin_lake(couche, "_journal"))

    def test_une_ligne_par_execution_avec_les_compteurs(self):
        c = journal.Compteurs(lues=10, ecrites=7, rejetees=2, dedoublonnees=1)
        journal.ecrire_journal(self.spark, "silver", "silver_x", "s2", "etablissement", "b1", c, "ok", DEBUT)
        ligne = self.lire("silver").first()
        self.assertEqual(
            (ligne.job, ligne.source, ligne.table, ligne.batch_id, ligne.statut),
            ("silver_x", "s2", "etablissement", "b1", "ok"),
        )
        self.assertEqual(
            (ligne.lignes_lues, ligne.lignes_ecrites, ligne.lignes_rejetees, ligne.lignes_dedoublonnees), (10, 7, 2, 1)
        )
        self.assertGreaterEqual(ligne.fin, ligne.debut)

    def test_un_journal_par_couche(self):
        journal.ecrire_journal(self.spark, "bronze", "j", "s2", "t", "b1", journal.Compteurs(), "ok", DEBUT)
        self.assertEqual(self.lire("bronze").count(), 1)
        self.assertFalse(os.path.exists(chemin_lake("silver", "_journal")))

    def test_un_journal_bronze_ancien_sans_colonne_dedoublonnees_continue_d_accepter_des_lignes(self):
        # schéma du journal Bronze avant l'ajout de lignes_dedoublonnees
        ancien = self.spark.createDataFrame(
            [("csv_bronze", "s2", "t", "b0", 5, 5, 0, "ok", None, DEBUT, DEBUT)],
            "job string, source string, `table` string, batch_id string, lignes_lues long, lignes_ecrites long, "
            "lignes_rejetees long, statut string, message string, debut timestamp, fin timestamp",
        )
        ajouter_delta(self.spark, ancien, chemin_lake("bronze", "_journal"))
        journal.ecrire_journal(self.spark, "bronze", "csv_bronze", "s2", "t", "b1", journal.Compteurs(lues=3), "ok", DEBUT)
        lignes = {r.batch_id: r.lignes_dedoublonnees for r in self.lire("bronze").collect()}
        self.assertEqual(lignes, {"b0": None, "b1": 0})


class TestRejets(AvecLac):
    def rejetees(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA_REJETEES)

    def table(self):
        return self.spark.read.format("delta").load(chemin_lake("silver", "_rejets", "s2"))

    def ecrire(self, rows, colonnes=("id", "code_postal")):
        rejets.ecrire_rejets(self.spark, self.rejetees(rows), "s2", "silver_x", DEBUT, colonnes)

    def test_schema_des_rejets_bronze_et_json_de_la_ligne(self):
        self.ecrire([("id_manquant", None, "1 rue Synthétique", "75008", "b1", "f.csv")])
        self.assertEqual(
            self.table().columns, ["motif", "_ingestion_ts", "job", "_fichier_origine", "_batch_id", "ligne_origine"]
        )
        r = self.table().first()
        self.assertEqual((r.motif, r.job, r._batch_id, r._fichier_origine), ("id_manquant", "silver_x", "b1", "f.csv"))
        self.assertEqual(json.loads(r.ligne_origine), {"code_postal": "75008"})  # null omis

    def test_seules_les_colonnes_autorisees_sont_recopiees(self):
        self.ecrire([("m", "42", "1 rue Synthétique", "75008", "b1", "f.csv")])
        contenu = json.loads(self.table().first().ligne_origine)
        self.assertEqual(contenu, {"id": "42", "code_postal": "75008"})
        self.assertNotIn("Synthétique", self.table().first().ligne_origine)

    def test_rejouer_le_meme_lot_n_ajoute_rien(self):
        rows = [("m", "1", "a", "75008", "b1", "f.csv"), ("m", "2", "a", "69001", "b1", "f.csv")]
        self.ecrire(rows)
        self.ecrire(rows)
        self.assertEqual(self.table().count(), 2)

    def test_un_nouveau_lot_ou_un_nouveau_motif_s_ajoute(self):
        self.ecrire([("m", "1", "a", "75008", "b1", "f.csv")])
        self.ecrire([("m", "1", "a", "75008", "b2", "f.csv"), ("autre_motif", "1", "a", "75008", "b1", "f.csv")])
        self.assertEqual(self.table().count(), 3)


if __name__ == "__main__":
    unittest.main()
