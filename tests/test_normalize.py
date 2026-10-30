"""Tests de pipelines/common/normalize.py : Spark local, données synthétiques.

    python -m unittest tests.test_normalize
"""

import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.common import normalize


class TestNormalize(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # session minimale : ces fonctions n'ont besoin ni de Delta ni de S3A
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_normalize")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def appliquer(self, fonction, valeurs):
        """Applique `fonction` à une colonne de textes et renvoie les résultats dans l'ordre des valeurs."""
        df = self.spark.createDataFrame([(i, v) for i, v in enumerate(valeurs)], "i int, v string")
        return [r.r for r in df.select("i", fonction(F.col("v")).alias("r")).orderBy("i").collect()]

    def verifier(self, fonction, cas):
        obtenus = self.appliquer(fonction, [entree for entree, _ in cas])
        for (entree, attendu), obtenu in zip(cas, obtenus):
            with self.subTest(entree=entree):
                self.assertEqual(obtenu, attendu)

    def test_finess_restitue_les_zeros_et_controle_le_format(self):
        self.verifier(normalize.normaliser_finess, [
            ("123456789", "123456789"),
            ("012345678", "012345678"),
            ("12345678", "012345678"),    # zéro initial perdu quand le code a été lu comme un nombre
            (" 123456789 ", "123456789"),
            ("2A0000012", "2A0000012"),   # FINESS corse : une lettre est valide en tête
            ("2a0000012", "2A0000012"),
            ("1234567890", None),         # trop long : rejeté, jamais tronqué
            ("12345678A", None),
            ("2A12", None),
            ("", None),
            ("   ", None),
            ("0", None),
            ("000000000", None),
            (None, None),
        ])

    def test_insee_restitue_les_zeros_et_controle_le_format(self):
        self.verifier(normalize.normaliser_insee, [
            ("75056", "75056"),
            ("01001", "01001"),           # un zéro initial n'est jamais retiré
            ("1001", "01001"),
            (" 1001 ", "01001"),
            ("2A004", "2A004"),
            ("2b033", "2B033"),
            ("123456", None),             # trop long : rejeté, jamais tronqué
            ("ABCDE", None),
            ("", None),
            ("0", None),
            ("00000", None),
            (None, None),
        ])

    def test_code_postal_restitue_les_zeros_et_controle_le_format(self):
        self.verifier(normalize.normaliser_code_postal, [
            ("75008", "75008"),
            ("1000", "01000"),            # zéro initial perdu
            (" 20137 ", "20137"),
            ("2A004", None),              # pas de lettre dans un code postal, même corse
            ("750081", None),
            ("", None),
            ("00000", None),
            (None, None),
        ])

    def test_libelle_majuscules_sans_accents(self):
        self.verifier(normalize.normaliser_libelle_jointure, [
            ("Hôpital Saint-Joseph", "HOPITAL SAINT-JOSEPH"),
            ("centre hospitalier d'Évry", "CENTRE HOSPITALIER D'EVRY"),
            ("Françoise Ç", "FRANCOISE C"),
            ("Straße", "STRASSE"),
            ("  Saint   Denis ", "SAINT DENIS"),
            ("Rue Pasteur", "RUE PASTEUR"),  # espace insécable
            ("", None),
            ("   ", None),
            (None, None),
        ])

    def test_identifiant_professionnel_sans_zeros_initiaux(self):
        self.verifier(normalize.normaliser_identifiant_professionnel, [
            ("00012345", "12345"),
            ("12345", "12345"),
            ("10200", "10200"),           # seuls les zéros du début partent
            (" 0123 ", "123"),
            ("0", None),
            ("000", None),
            ("", None),
            (None, None),
        ])

    def cles(self, *colonnes_valeurs):
        """Calcule cle_substitution sur des lignes de textes ; chaque ligne est un tuple de valeurs."""
        n = len(colonnes_valeurs[0])
        schema = ", ".join(f"c{j} string" for j in range(n))
        df = self.spark.createDataFrame(list(colonnes_valeurs), schema)
        cles = normalize.cle_substitution(*[F.col(f"c{j}") for j in range(n)])
        return [r.k for r in df.select(cles.alias("k")).collect()]

    def test_cle_deterministe_et_positive(self):
        premiere, seconde = self.cles(("F123456789",)), self.cles(("F123456789",))
        self.assertEqual(premiere, seconde)
        self.assertGreaterEqual(premiere[0], 0)  # -1 reste réservé au membre « Non renseigné »

    def test_cle_valeur_figee(self):
        # si cette valeur change, toutes les clés étrangères des faits changent : pas sans migration
        self.assertEqual(self.cles(("F123456789",)), [8593418689730937385])

    def test_cle_distingue_les_valeurs_et_l_ordre_des_colonnes(self):
        cles = self.cles(("A",), ("B",))
        self.assertNotEqual(cles[0], cles[1])
        cles = self.cles(("a", "b"), ("b", "a"), ("ab", "c"), ("a", "bc"))
        self.assertEqual(len(set(cles)), 4)

    def test_cle_depend_de_la_casse_donc_la_cle_metier_se_normalise_avant(self):
        cles = self.cles(("F123456789",), ("f123456789",))
        self.assertNotEqual(cles[0], cles[1])

    def test_cle_independante_du_type_de_la_source(self):
        a = self.spark.createDataFrame([(123,)], "c0 int").select(normalize.cle_substitution(F.col("c0"))).first()[0]
        b = self.spark.createDataFrame([("123",)], "c0 string").select(normalize.cle_substitution(F.col("c0"))).first()[0]
        self.assertEqual(a, b)

    def test_cle_nulle_si_une_colonne_est_nulle(self):
        self.assertEqual(self.cles((None,), ("A",))[0], None)
        self.assertIsNone(self.cles(("a", None))[0])


if __name__ == "__main__":
    unittest.main()
