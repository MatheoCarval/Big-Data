"""Client FTP des sources S3 (satisfaction) et S4 (décès), bibliothèque standard uniquement.

Côté pipelines (compte lecture seule FTP_USER) :
    python -m pipelines.common.ftp lister satisfaction
    python -m pipelines.common.ftp telecharger satisfaction data/ftp/satisfaction

Côté simulation du producteur (compte FTP_DEPOT_USER) :
    python -m pipelines.common.ftp initialiser "chemin/vers/DATA 2024"
    python -m pipelines.common.ftp deposer fichier.csv satisfaction/2021

Les transferts passent par un fichier temporaire « .part » renommé à la fin : un capteur
ne voit jamais un fichier à moitié écrit. Un fichier déjà présent avec la même taille
est ignoré, ce qui rend dépôt et téléchargement relançables sans doublon.
"""

import argparse
import ftplib
import os
import posixpath
import socket
import sys
from pathlib import Path

SUFFIXE_PARTIEL = ".part"
TENTATIVES = 5
# correspondance DATA 2024 -> arborescence FTP
SOURCES_DEPOT = {
    "Satisfaction": "satisfaction",
    "DECES EN FRANCE": "deces",
}


def connecter(depot=False):
    prefixe = "FTP_DEPOT" if depot else "FTP"
    try:
        utilisateur, mot_de_passe = os.environ[f"{prefixe}_USER"], os.environ[f"{prefixe}_PASSWORD"]
    except KeyError as e:
        raise SystemExit(f"Variable {e} absente : charger le .env (set -a; . ./.env; set +a)")
    ftp = ftplib.FTP(timeout=60)
    ftp.connect(os.environ.get("FTP_HOST", "localhost"), int(os.environ.get("FTP_PORT", "2121")))
    ftp.login(utilisateur, mot_de_passe)
    return ftp


def taille_distante(ftp, chemin):
    try:
        ftp.voidcmd("TYPE I")
        return ftp.size(chemin)
    except ftplib.error_perm:
        return None


def creer_dossiers(ftp, dossier):
    courant = ""
    for partie in [p for p in dossier.split("/") if p]:
        courant = f"{courant}/{partie}"
        try:
            ftp.mkd(courant)
        except ftplib.error_perm:
            pass  # existe déjà


def lister(ftp, dossier):
    """Chemins de tous les fichiers sous `dossier`, récursivement : [(chemin, taille)]."""
    fichiers = []
    for nom, faits in ftp.mlsd(dossier, facts=["type", "size"]):
        chemin = posixpath.join(dossier, nom)
        if faits["type"] == "dir":
            fichiers += lister(ftp, chemin)
        elif faits["type"] == "file" and not nom.endswith(SUFFIXE_PARTIEL):
            fichiers.append((chemin, int(faits["size"])))
    return fichiers


def envoyer(ftp, local, cible):
    """STOR vérifié, repris par APPE tant que le serveur n'a pas reçu tout le fichier.

    À travers la redirection de ports de Docker Desktop, la fin d'un transfert est
    parfois perdue (constaté sous Windows) : on contrôle la taille reçue et on reprend.
    """
    taille = local.stat().st_size
    recu = 0
    with local.open("rb") as f:
        for _ in range(TENTATIVES):
            f.seek(recu)
            ftp.voidcmd("TYPE I")
            with ftp.transfercmd(f"{'APPE' if recu else 'STOR'} {cible}") as conn:
                while bloc := f.read(1 << 20):
                    conn.sendall(bloc)
                conn.shutdown(socket.SHUT_WR)
                conn.settimeout(300)
                conn.recv(1)  # le serveur ferme quand il a fini de lire
            ftp.voidresp()
            recu = taille_distante(ftp, cible) or 0
            if recu == taille:
                return
    raise IOError(f"{cible} : {recu} octets reçus sur {taille} après {TENTATIVES} tentatives")


def recevoir(ftp, chemin, local, taille):
    """RETR vérifié, repris par REST si le fichier local est incomplet."""
    with local.open("wb") as f:
        for _ in range(TENTATIVES):
            ftp.retrbinary(f"RETR {chemin}", f.write, blocksize=1 << 20, rest=f.tell() or None)
            f.flush()
            if f.tell() == taille:
                return
    raise IOError(f"{chemin} : {local.stat().st_size} octets reçus sur {taille}")


def deposer(ftp, local, dossier_distant):
    """Dépose un fichier local dans `dossier_distant`. Renvoie False s'il y était déjà."""
    local = Path(local)
    cible = posixpath.join("/", dossier_distant.strip("/"), local.name)
    if taille_distante(ftp, cible) == local.stat().st_size:
        return False
    creer_dossiers(ftp, posixpath.dirname(cible))
    envoyer(ftp, local, cible + SUFFIXE_PARTIEL)
    try:
        ftp.delete(cible)
    except ftplib.error_perm:
        pass
    ftp.rename(cible + SUFFIXE_PARTIEL, cible)
    return True


def telecharger(ftp, dossier_distant, dossier_local):
    """Recopie récursivement `dossier_distant` dans `dossier_local`. Renvoie les fichiers récupérés."""
    racine = posixpath.join("/", dossier_distant.strip("/"))
    recuperes = []
    for chemin, taille in lister(ftp, racine):
        local = Path(dossier_local, posixpath.relpath(chemin, racine))
        if local.exists() and local.stat().st_size == taille:
            continue
        local.parent.mkdir(parents=True, exist_ok=True)
        partiel = local.with_name(local.name + SUFFIXE_PARTIEL)
        recevoir(ftp, chemin, partiel, taille)
        partiel.replace(local)
        recuperes.append(local)
    return recuperes


def initialiser(ftp, data_2024):
    """Dépose les sources S3 et S4 du pack DATA 2024 en conservant leur arborescence."""
    data_2024 = Path(data_2024)
    deposes = ignores = 0
    for dossier_local, dossier_ftp in SOURCES_DEPOT.items():
        racine = data_2024 / dossier_local
        if not racine.is_dir():
            raise SystemExit(f"Dossier introuvable : {racine}")
        for fichier in sorted(p for p in racine.rglob("*") if p.is_file() and not p.name.startswith("~$")):
            relatif = fichier.parent.relative_to(racine).as_posix()
            distant = posixpath.normpath(posixpath.join(dossier_ftp, relatif))
            if deposer(ftp, fichier, distant):
                deposes += 1
                print(f"+ {distant}/{fichier.name}")
            else:
                ignores += 1
    print(f"{deposes} fichiers déposés, {ignores} déjà présents")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sous = parser.add_subparsers(dest="commande", required=True)
    p = sous.add_parser("lister", help="liste les fichiers d'un dossier FTP")
    p.add_argument("dossier", nargs="?", default="/")
    p = sous.add_parser("telecharger", help="recopie un dossier FTP en local")
    p.add_argument("dossier")
    p.add_argument("destination")
    p = sous.add_parser("deposer", help="dépose un fichier (compte de dépôt)")
    p.add_argument("fichier")
    p.add_argument("dossier")
    p = sous.add_parser("initialiser", help="dépose S3 et S4 depuis DATA 2024 (compte de dépôt)")
    p.add_argument("data_2024", nargs="?", default="data")
    args = parser.parse_args(argv)

    with connecter(depot=args.commande in ("deposer", "initialiser")) as ftp:
        if args.commande == "lister":
            for chemin, taille in lister(ftp, posixpath.join("/", args.dossier.strip("/"))):
                print(f"{taille:>14}  {chemin}")
        elif args.commande == "telecharger":
            recuperes = telecharger(ftp, args.dossier, args.destination)
            print(f"{len(recuperes)} fichiers téléchargés dans {args.destination}")
        elif args.commande == "deposer":
            deja = not deposer(ftp, args.fichier, args.dossier)
            print(f"{'déjà présent' if deja else 'déposé'} : {args.dossier}/{Path(args.fichier).name}")
        else:
            initialiser(ftp, args.data_2024)


if __name__ == "__main__":
    sys.exit(main())
