"""Tests de pipelines/common/delta_io.py : données synthétiques.

    python -m unittest tests.test_delta_io

Les deux classes se lancent dans Docker (voir CLAUDE.md §13) : TestFusionner écrit des tables Delta.
"""

import os
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest import mock

from delta.tables import DeltaTable

from pipelines.common import delta_io
from pipelines.common.session_spark import creer_session

SCHEMA = "id string, valeur string, _ingestion_ts timestamp, _batch_id string"


def ts(jour):
    return datetime(2026, 10, jour)


class TestDedoublonner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # session du projet, Delta inclus : la JVM n'est lancée qu'une fois par processus (voir test_normalize)
        with mock.patch.dict(os.environ, {"LAKE_ROOT": tempfile.gettempdir()}):
            cls.spark = creer_session("test_dedoublonner")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def lignes(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA)

    def test_garde_la_ligne_la_plus_recente(self):
        df = self.lignes([("a", "vieux", ts(1), "b1"), ("a", "recent", ts(2), "b2"), ("b", "seul", ts(1), "b1")])
        uniques, ecartes = delta_io.dedoublonner(df, "id")
        self.assertEqual({r.id: r.valeur for r in uniques.collect()}, {"a": "recent", "b": "seul"})
        self.assertEqual(ecartes, 1)

    def test_sans_doublon_rien_n_est_ecarte(self):
        df = self.lignes([("a", "x", ts(1), "b1"), ("b", "y", ts(1), "b1")])
        uniques, ecartes = delta_io.dedoublonner(df, ["id"])
        self.assertEqual(uniques.count(), 2)
        self.assertEqual(ecartes, 0)

    def test_aucune_colonne_de_travail_ne_reste(self):
        df = self.lignes([("a", "x", ts(1), "b1")])
        uniques, _ = delta_io.dedoublonner(df, "id")
        self.assertEqual(uniques.columns, df.columns)

    def test_egalite_une_seule_ligne_et_choix_independant_de_l_ordre(self):
        a, b = ("k", "premiere", ts(1), "b1"), ("k", "seconde", ts(1), "b1")
        gardees = set()
        for rows in ([a, b], [b, a]):
            uniques, ecartes = delta_io.dedoublonner(self.lignes(rows), "id")
            self.assertEqual(uniques.count(), 1)
            self.assertEqual(ecartes, 1)
            gardees.add(uniques.first().valeur)
        self.assertEqual(len(gardees), 1)

    def test_cle_composite(self):
        df = self.lignes([("a", "x", ts(1), "b1"), ("a", "x", ts(2), "b2"), ("a", "y", ts(1), "b1")])
        uniques, ecartes = delta_io.dedoublonner(df, ["id", "valeur"])
        self.assertEqual(uniques.count(), 2)
        self.assertEqual(ecartes, 1)


class TestFusionner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # session locale : sans LAKE_ROOT, le défaut s3a:// exigerait la configuration MinIO
        with mock.patch.dict(os.environ, {"LAKE_ROOT": tempfile.gettempdir()}):
            cls.spark = creer_session("test_delta_io")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.dossier = tempfile.mkdtemp()
        self.chemin = os.path.join(self.dossier, "table")

    def tearDown(self):
        shutil.rmtree(self.dossier, ignore_errors=True)

    def charger(self, rows, cles="id"):
        delta_io.fusionner(self.spark, self.spark.createDataFrame(rows, SCHEMA), self.chemin, cles)

    def contenu(self):
        return {r.id: r.valeur for r in self.spark.read.format("delta").load(self.chemin).collect()}

    def version(self):
        return DeltaTable.forPath(self.spark, self.chemin).history(1).first()["version"]

    def lignes_ecrites_depuis(self, version):
        """Lignes insérées ou mises à jour par les commits postérieurs à `version`."""
        total = 0
        for h in DeltaTable.forPath(self.spark, self.chemin).history().collect():
            if h["version"] > version and h["operation"] == "MERGE":
                m = h["operationMetrics"]
                total += int(m["numTargetRowsInserted"]) + int(m["numTargetRowsUpdated"])
        return total

    def test_premiere_charge_cree_la_table(self):
        self.charger([("a", "x", ts(1), "b1"), ("b", "y", ts(1), "b1")])
        self.assertEqual(self.contenu(), {"a": "x", "b": "y"})

    def test_rejouer_le_meme_lot_ne_change_rien(self):
        lot = [("a", "x", ts(1), "b1"), ("b", "y", ts(1), "b1")]
        self.charger(lot)
        version = self.version()
        self.charger(lot)
        self.assertEqual(self.contenu(), {"a": "x", "b": "y"})
        self.assertEqual(self.lignes_ecrites_depuis(version), 0)

    def test_ligne_plus_recente_met_a_jour_sans_doublon(self):
        self.charger([("a", "x", ts(1), "b1")])
        self.charger([("a", "x2", ts(2), "b2")])
        self.assertEqual(self.contenu(), {"a": "x2"})
        self.assertEqual(self.spark.read.format("delta").load(self.chemin).count(), 1)

    def test_lot_plus_ancien_n_ecrase_pas_un_lot_recent(self):
        self.charger([("a", "recent", ts(5), "b5")])
        self.charger([("a", "ancien", ts(1), "b1")])
        self.assertEqual(self.contenu(), {"a": "recent"})

    def test_nouvelles_cles_sont_inserees(self):
        self.charger([("a", "x", ts(1), "b1")])
        self.charger([("a", "x", ts(1), "b1"), ("c", "z", ts(2), "b2")])
        self.assertEqual(self.contenu(), {"a": "x", "c": "z"})

    def test_cle_composite(self):
        self.charger([("a", "x", ts(1), "b1"), ("a", "y", ts(1), "b1")], cles=["id", "valeur"])
        self.charger([("a", "x", ts(2), "b2")], cles=["id", "valeur"])
        table = self.spark.read.format("delta").load(self.chemin)
        self.assertEqual(table.count(), 2)
        self.assertEqual(table.filter("valeur = 'x'").first()._ingestion_ts, ts(2))

    def test_doublons_du_lot_refuses_quand_la_cle_existe_deja(self):
        self.charger([("a", "x", ts(1), "b1")])
        with self.assertRaises(Exception):
            self.charger([("a", "x2", ts(2), "b2"), ("a", "x3", ts(2), "b2")])

    def test_doublons_du_lot_non_detectes_sur_table_vide(self):
        # limite de Delta : c'est pour cela que dedoublonner passe toujours avant fusionner
        self.charger([("a", "x2", ts(2), "b2"), ("a", "x3", ts(2), "b2")])
        self.assertEqual(self.spark.read.format("delta").load(self.chemin).count(), 2)

    def test_renvoie_les_lignes_inserees_et_mises_a_jour(self):
        def fusion(rows):
            return delta_io.fusionner(self.spark, self.spark.createDataFrame(rows, SCHEMA), self.chemin, "id")

        self.assertEqual(fusion([("a", "x", ts(1), "b1"), ("b", "y", ts(1), "b1")]), (2, 0))
        self.assertEqual(fusion([("a", "x", ts(1), "b1"), ("b", "y", ts(1), "b1")]), (0, 0))  # rejeu
        self.assertEqual(fusion([("a", "x2", ts(2), "b2"), ("c", "z", ts(2), "b2")]), (1, 1))
        self.assertEqual(fusion([("a", "ancien", ts(1), "b0")]), (0, 0))  # lot plus ancien


class TestAjouterEtLireLot(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with mock.patch.dict(os.environ, {"LAKE_ROOT": tempfile.gettempdir()}):
            cls.spark = creer_session("test_ajouter_lire_lot")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.dossier = tempfile.mkdtemp()
        self.chemin = os.path.join(self.dossier, "table")

    def tearDown(self):
        shutil.rmtree(self.dossier, ignore_errors=True)

    def lignes(self, rows, schema=SCHEMA):
        return self.spark.createDataFrame(rows, schema)

    def test_ajouter_cumule_les_lots_et_la_table_est_en_ajout_seul(self):
        delta_io.ajouter_delta(self.spark, self.lignes([("a", "x", ts(1), "b1")]), self.chemin)
        delta_io.ajouter_delta(self.spark, self.lignes([("b", "y", ts(2), "b2")]), self.chemin)
        table = self.spark.read.format("delta").load(self.chemin)
        self.assertEqual(sorted(r.id for r in table.collect()), ["a", "b"])
        with self.assertRaises(Exception):
            DeltaTable.forPath(self.spark, self.chemin).delete("id = 'a'")

    def test_colonne_en_plus_refusee_sauf_evolution_de_schema(self):
        delta_io.ajouter_delta(self.spark, self.lignes([("a", "x", ts(1), "b1")]), self.chemin)
        plus = self.lignes([("b", "y", ts(2), "b2", 7)], SCHEMA + ", extra int")
        with self.assertRaises(Exception):
            delta_io.ajouter_delta(self.spark, plus, self.chemin)
        delta_io.ajouter_delta(self.spark, plus, self.chemin, evolution_schema=True)
        table = self.spark.read.format("delta").load(self.chemin)
        self.assertEqual({r.id: r.extra for r in table.collect()}, {"a": None, "b": 7})

    def test_dernier_lot_ne_renvoie_que_le_lot_le_plus_recent(self):
        delta_io.ajouter_delta(self.spark, self.lignes([("a", "v1", ts(1), "b1"), ("b", "v1", ts(1), "b1")]), self.chemin)
        delta_io.ajouter_delta(self.spark, self.lignes([("a", "v2", ts(5), "b2")]), self.chemin)
        lot, batch_id = delta_io.lire_dernier_lot(self.spark, self.chemin)
        self.assertEqual(batch_id, "b2")
        self.assertEqual([(r.id, r.valeur) for r in lot.collect()], [("a", "v2")])

    def test_dernier_lot_d_une_table_vide_est_une_erreur_claire(self):
        delta_io.ajouter_delta(self.spark, self.lignes([], SCHEMA), self.chemin)
        with self.assertRaises(ValueError):
            delta_io.lire_dernier_lot(self.spark, self.chemin)


if __name__ == "__main__":
    unittest.main()
