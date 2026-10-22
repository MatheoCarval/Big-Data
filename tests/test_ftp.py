"""Tests du client FTP sur un serveur pyftpdlib local et des fichiers synthétiques.

    python -m unittest tests.test_ftp
"""

import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from pipelines.common import ftp as client

try:
    from pyftpdlib.authorizers import DummyAuthorizer
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer
except ImportError:
    FTPServer = None


@unittest.skipIf(FTPServer is None, "pyftpdlib non installé")
class TestFtp(unittest.TestCase):
    def setUp(self):
        self.racine = Path(tempfile.mkdtemp())
        self.local = Path(tempfile.mkdtemp())
        auth = DummyAuthorizer()
        auth.add_user("lecteur", "l", str(self.racine), perm="elr")
        auth.add_user("depot", "d", str(self.racine), perm="elradfmwMT")
        handler = type("H", (FTPHandler,), {"authorizer": auth})
        self.serveur = FTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.serveur.serve_forever, kwargs={"timeout": 0.1}, daemon=True).start()
        port = self.serveur.socket.getsockname()[1]
        self.env = mock.patch.dict(os.environ, {
            "FTP_HOST": "127.0.0.1", "FTP_PORT": str(port),
            "FTP_USER": "lecteur", "FTP_PASSWORD": "l",
            "FTP_DEPOT_USER": "depot", "FTP_DEPOT_PASSWORD": "d",
        })
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.serveur.close_all()

    def ecrire(self, relatif, contenu):
        chemin = self.local / "source" / relatif
        chemin.parent.mkdir(parents=True, exist_ok=True)
        chemin.write_text(contenu, encoding="utf-8")
        return chemin

    def test_initialiser_puis_telecharger(self):
        self.ecrire("Satisfaction/2020/scores.csv", "finess;score\n000000001;80\n")
        self.ecrire("Satisfaction/lexique.csv", "colonne;libelle\n")
        self.ecrire("DECES EN FRANCE/deces.csv", "nom;date\nX;2019-01-01\n")

        with client.connecter(depot=True) as ftp:
            client.initialiser(ftp, self.local / "source")
        self.assertTrue((self.racine / "satisfaction/2020/scores.csv").exists())
        self.assertTrue((self.racine / "satisfaction/lexique.csv").exists())
        self.assertTrue((self.racine / "deces/deces.csv").exists())

        with client.connecter() as ftp:
            recuperes = client.telecharger(ftp, "satisfaction", self.local / "miroir")
        self.assertEqual(len(recuperes), 2)
        self.assertEqual((self.local / "miroir/2020/scores.csv").read_text(encoding="utf-8"),
                         "finess;score\n000000001;80\n")

    def test_relancer_ne_retransfere_rien(self):
        fichier = self.ecrire("a.csv", "x\n1\n")
        with client.connecter(depot=True) as ftp:
            self.assertTrue(client.deposer(ftp, fichier, "satisfaction/2021"))
            self.assertFalse(client.deposer(ftp, fichier, "satisfaction/2021"))
        with client.connecter() as ftp:
            self.assertEqual(len(client.telecharger(ftp, "satisfaction", self.local / "m")), 1)
            self.assertEqual(client.telecharger(ftp, "satisfaction", self.local / "m"), [])

    def test_fichier_partiel_ignore(self):
        (self.racine / "deces").mkdir()
        (self.racine / "deces/deces.csv.part").write_text("en cours", encoding="utf-8")
        with client.connecter() as ftp:
            self.assertEqual(client.lister(ftp, "/deces"), [])

    def test_compte_lecture_seule(self):
        fichier = self.ecrire("a.csv", "x\n")
        with client.connecter() as ftp:
            with self.assertRaises(client.ftplib.error_perm):
                client.deposer(ftp, fichier, "satisfaction")


if __name__ == "__main__":
    unittest.main()
