"""Serveur FTP simulant le dépôt des sources S3 (satisfaction) et S4 (décès INSEE).

Deux comptes :
- FTP_USER : lecture seule, utilisé par les pipelines et Airflow ;
- FTP_DEPOT_USER : lecture / écriture, simule le producteur qui dépose les fichiers.

Arborescence servie :
    /satisfaction/<annee>/<fichier>
    /deces/<fichier>
"""

import logging
import os

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

RACINE = "/srv/ftp"
DOSSIERS = ("satisfaction", "deces")
PORTS_PASSIFS = range(30000, 30010)


class Handler(FTPHandler):
    def on_file_received(self, fichier):
        logging.info("fichier déposé : %s", os.path.relpath(fichier, RACINE))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for dossier in DOSSIERS:
        os.makedirs(os.path.join(RACINE, dossier), exist_ok=True)

    auth = DummyAuthorizer()
    auth.add_user(os.environ["FTP_USER"], os.environ["FTP_PASSWORD"], RACINE, perm="elr")
    auth.add_user(os.environ["FTP_DEPOT_USER"], os.environ["FTP_DEPOT_PASSWORD"], RACINE, perm="elradfmwMT")

    Handler.authorizer = auth
    Handler.passive_ports = PORTS_PASSIFS
    Handler.banner = "FTP de simulation CHU (sources S3 et S4)"

    serveur = FTPServer(("0.0.0.0", 21), Handler)
    serveur.max_cons = 50
    serveur.serve_forever()


if __name__ == "__main__":
    main()
