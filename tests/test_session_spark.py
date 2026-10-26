"""Tests de pipelines/common/session_spark.py sans Spark : un faux builder enregistre la configuration.

    python -m unittest tests.test_session_spark
"""

import os
import unittest
from unittest import mock

from pipelines.common import session_spark

ENV_S3 = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ROOT_USER": "utilisateur-test",
    "MINIO_ROOT_PASSWORD": "mot-de-passe-test",
}


class FauxBuilder:
    def __init__(self):
        self.options = {}

    def config(self, cle, valeur):
        self.options[cle] = valeur
        return self


class TestLacSurS3(unittest.TestCase):
    def test_s3_par_defaut(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(session_spark.lac_sur_s3())

    def test_variable_vide_vaut_defaut(self):
        with mock.patch.dict(os.environ, {"LAKE_ROOT": ""}, clear=True):
            self.assertTrue(session_spark.lac_sur_s3())

    def test_racine_s3a_explicite(self):
        with mock.patch.dict(os.environ, {"LAKE_ROOT": "s3a://"}, clear=True):
            self.assertTrue(session_spark.lac_sur_s3())

    def test_dossier_local_desactive_s3(self):
        with mock.patch.dict(os.environ, {"LAKE_ROOT": "/tmp/lac"}, clear=True):
            self.assertFalse(session_spark.lac_sur_s3())


class TestConfigurerS3a(unittest.TestCase):
    def test_pointe_vers_minio_en_adressage_par_chemin(self):
        with mock.patch.dict(os.environ, ENV_S3, clear=True):
            options = session_spark.configurer_s3a(FauxBuilder()).options
        self.assertEqual(options["spark.hadoop.fs.s3a.endpoint"], "http://minio:9000")
        self.assertEqual(options["spark.hadoop.fs.s3a.access.key"], "utilisateur-test")
        self.assertEqual(options["spark.hadoop.fs.s3a.secret.key"], "mot-de-passe-test")
        self.assertEqual(options["spark.hadoop.fs.s3a.path.style.access"], "true")
        self.assertEqual(options["spark.hadoop.fs.s3a.connection.ssl.enabled"], "false")
        self.assertEqual(options["spark.hadoop.fs.s3a.impl"], "org.apache.hadoop.fs.s3a.S3AFileSystem")

    def test_tls_active_si_endpoint_https(self):
        env = {**ENV_S3, "MINIO_ENDPOINT": "https://minio.exemple:9000"}
        with mock.patch.dict(os.environ, env, clear=True):
            options = session_spark.configurer_s3a(FauxBuilder()).options
        self.assertEqual(options["spark.hadoop.fs.s3a.connection.ssl.enabled"], "true")

    def test_variable_manquante_leve_une_erreur_explicite(self):
        for manquante in ENV_S3:
            env = {k: v for k, v in ENV_S3.items() if k != manquante}
            with self.subTest(manquante=manquante), mock.patch.dict(os.environ, env, clear=True):
                with self.assertRaises(KeyError) as ctx:
                    session_spark.configurer_s3a(FauxBuilder())
                self.assertEqual(ctx.exception.args[0], manquante)


if __name__ == "__main__":
    unittest.main()
