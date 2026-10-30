"""Tests de pipelines/common/referentiels.py : Spark local, données synthétiques.

    python -m unittest tests.test_referentiels
"""

import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.common import referentiels

REGIONS_METROPOLE = {"11", "24", "27", "28", "32", "44", "52", "53", "75", "76", "84", "93", "94"}


class TestDictionnaire(unittest.TestCase):
    def test_metropole_complete(self):
        codes = [c for c in referentiels.dictionnaire_departement() if len(c) == 2 and c != "99"]
        attendus = [f"{n:02d}" for n in range(1, 96) if n != 20] + ["2A", "2B"]
        self.assertCountEqual(codes, attendus)  # 96 départements : 01 à 95 sans 20, plus 2A et 2B

    def test_un_code_region_n_a_qu_un_seul_libelle(self):
        libelles = {}
        for _, code_region, libelle_region in referentiels.dictionnaire_departement().values():
            libelles.setdefault(code_region, set()).add(libelle_region)
        for code_region, noms in libelles.items():
            with self.subTest(code_region=code_region):
                self.assertEqual(len(noms), 1, noms)

    def test_treize_regions_metropolitaines(self):
        regions = {
            code_region
            for code, (_, code_region, _) in referentiels.dictionnaire_departement().items()
            if len(code) == 2 and code != "99"
        }
        self.assertEqual(regions, REGIONS_METROPOLE)

    def test_outre_mer_et_etranger(self):
        dico = referentiels.dictionnaire_departement()
        self.assertEqual({dico[c][1] for c in ("971", "972", "973", "974", "976")}, {"01", "02", "03", "04", "06"})
        self.assertEqual({dico[c][1] for c in ("975", "977", "978", "984", "986", "987", "988", "989")}, {"COM"})
        self.assertEqual(dico["99"][1], "99")


class TestRecherche(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_referentiels")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def departements(self, cas):
        """cas : liste de (code_commune, code_postal, département attendu)."""
        df = self.spark.createDataFrame([(i, c, p) for i, (c, p, _) in enumerate(cas)], "i int, c string, p string")
        lignes = df.select("i", referentiels.departement_de_commune(F.col("c"), F.col("p")).alias("d")).orderBy("i")
        for (commune, postal, attendu), ligne in zip(cas, lignes.collect()):
            with self.subTest(commune=commune, postal=postal):
                self.assertEqual(ligne.d, attendu)

    def test_la_commune_prime_sur_le_code_postal(self):
        self.departements([("75056", "13001", "75"), ("13055", None, "13"), ("69123", "", "69")])

    def test_zero_initial_perdu_ne_donne_pas_un_autre_departement(self):
        # « 1001 » est 01001 (Ain) : lu tel quel, il commencerait par 10 (Aube)
        self.departements([("1001", None, "01"), (None, "1000", "01")])

    def test_corse_par_la_commune(self):
        self.departements([("2A004", None, "2A"), ("2b033", None, "2B")])

    def test_corse_par_le_code_postal(self):
        self.departements([
            (None, "20000", "2A"), (None, "20137", "2A"), (None, "20199", "2A"),
            (None, "20200", "2B"), (None, "20600", "2B"),
        ])

    def test_ancien_code_corse_passe_au_code_postal(self):
        self.departements([("20004", "20000", "2A"), ("20004", None, None)])

    def test_outre_mer_et_etranger(self):
        self.departements([
            ("97105", None, "971"), ("97611", None, "976"), ("98818", None, "988"), ("99350", None, "99"),
            (None, "97100", "971"), (None, "97600", "976"), (None, "98800", "988"),
        ])

    def test_commune_invalide_passe_au_code_postal(self):
        self.departements([("ABCDE", "69001", "69"), ("123456", "33000", "33"), ("00000", "59000", "59")])

    def test_rien_d_exploitable(self):
        self.departements([(None, None, None), ("", "", None), ("ABCDE", "XY", None), ("00000", "00000", None)])

    def attributs(self, fonction, cas):
        df = self.spark.createDataFrame([(i, d) for i, (d, _) in enumerate(cas)], "i int, d string")
        lignes = df.select("i", fonction(F.col("d")).alias("r")).orderBy("i").collect()
        for (departement, attendu), ligne in zip(cas, lignes):
            with self.subTest(departement=departement):
                self.assertEqual(ligne.r, attendu)

    def test_code_region(self):
        self.attributs(referentiels.region_depuis_departement, [
            ("75", "11"), ("2A", "94"), ("33", "75"), ("971", "01"), ("976", "06"), ("988", "COM"), ("99", "99"),
            ("XX", None), ("", None), (None, None),
        ])

    def test_libelles(self):
        self.attributs(referentiels.libelle_departement, [("2B", "Haute-Corse"), ("974", "La Réunion"), ("ZZ", None)])
        self.attributs(referentiels.libelle_region, [("33", "Nouvelle-Aquitaine"), ("974", "La Réunion"), (None, None)])


if __name__ == "__main__":
    unittest.main()
