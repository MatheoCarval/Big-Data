"""Extraction S3 (satisfaction) : fichiers CSV / XLSX -> PostgreSQL, schéma bronze_s3.

Chaque fichier de données ou de lexique devient une table dont toutes les colonnes
sont en TEXT (aucune transformation, valeurs telles que lues), complétée des colonnes
techniques _ingestion_ts, _source, _batch_id, _fichier_origine, _ligne.
Un fichier déjà ingéré (même chemin, même empreinte SHA-256) est ignoré : le job
peut être relancé sans créer de doublon. Un fichier modifié est ajouté (append-only).

Aucune dépendance hors bibliothèque standard ; le chargement passe par `psql`.

    python3 -m pipelines.bronze.s3_satisfaction            # charge S3_DATA_DIR
    python3 -m pipelines.bronze.s3_satisfaction --ddl      # affiche le DDL, sans base
"""

import argparse
import csv
import hashlib
import io
import os
import re
import subprocess
import sys
import unicodedata
import uuid
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA = "bronze_s3"
SOURCE = "s3_satisfaction"
SCHEMA_SQL = Path(__file__).with_name("s3_schema.sql")
ENCODAGES = ("utf-8-sig", "cp1252", "latin-1")
COLONNES_TECHNIQUES = ("_ingestion_ts", "_source", "_batch_id", "_fichier_origine", "_ligne")

NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


@dataclass
class Fichier:
    chemin: Path
    relatif: str
    format: str
    table: str
    colonnes_origine: list = field(default_factory=list)
    colonnes: list = field(default_factory=list)
    lignes: list = field(default_factory=list)
    encodage: str = None
    separateur: str = None
    feuille: str = None
    sha256: str = None


def snake_case(texte):
    texte = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode()
    texte = re.sub(r"[^0-9a-zA-Z]+", "_", texte).strip("_").lower()
    return texte


def noms_colonnes(entete):
    """Noms SQL uniques, en snake_case, sans collision avec les colonnes techniques."""
    noms, vus = [], set(COLONNES_TECHNIQUES)
    for i, brut in enumerate(entete, 1):
        nom = snake_case(brut) or f"colonne_{i}"
        if nom[0].isdigit():
            nom = f"c_{nom}"
        nom = nom[:60]
        base, n = nom, 2
        while nom in vus:
            nom, n = f"{base}_{n}", n + 1
        vus.add(nom)
        noms.append(nom)
    return noms


def decoder(octets):
    for enc in ENCODAGES:
        try:
            return octets.decode(enc), enc
        except UnicodeDecodeError:
            continue
    raise ValueError("encodage non reconnu")


def detecter_separateur(entete):
    return max((";", ",", "\t", "|"), key=entete.count)


def lire_csv(octets):
    texte, encodage = decoder(octets)
    premiere = texte.splitlines()[0] if texte else ""
    sep = detecter_separateur(premiere)
    lignes = list(csv.reader(io.StringIO(texte, newline=""), delimiter=sep))
    return lignes, encodage, sep


def _col_index(ref):
    lettres = re.match(r"[A-Z]+", ref).group()
    n = 0
    for c in lettres:
        n = n * 26 + ord(c) - 64
    return n - 1


def _texte(el):
    # chaîne simple (<t>) ou riche (<r><t>…), la phonétique (<rPh>) est ignorée
    t = el.find("m:t", NS)
    if t is not None:
        return t.text or ""
    return "".join(r.findtext("m:t", "", NS) for r in el.findall("m:r", NS))


def lire_xlsx(octets):
    """Première feuille d'un classeur, valeurs brutes des cellules (sans dépendance)."""
    with zipfile.ZipFile(io.BytesIO(octets)) as z:
        partagees = []
        if "xl/sharedStrings.xml" in z.namelist():
            racine = ET.fromstring(z.read("xl/sharedStrings.xml"))
            partagees = [_texte(si) for si in racine.findall("m:si", NS)]
        classeur = ET.fromstring(z.read("xl/workbook.xml"))
        feuille = classeur.find("m:sheets/m:sheet", NS)
        rid = feuille.get(f"{{{NS['r']}}}id")
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        cible = next(r.get("Target") for r in rels.findall("rel:Relationship", NS) if r.get("Id") == rid)
        cible = cible.lstrip("/") if cible.startswith("/") else f"xl/{cible}"
        donnees = ET.fromstring(z.read(cible))

    lignes = []
    for row in donnees.iterfind("m:sheetData/m:row", NS):
        valeurs = {}
        for i, c in enumerate(row.findall("m:c", NS)):
            ref, typ = c.get("r"), c.get("t")
            idx = _col_index(ref) if ref else i
            if typ == "inlineStr":
                is_ = c.find("m:is", NS)
                val = _texte(is_) if is_ is not None else ""
            else:
                v = c.find("m:v", NS)
                val = v.text if v is not None and v.text is not None else ""
                if typ == "s" and val != "":
                    val = partagees[int(val)]
            valeurs[idx] = val
        if any(v != "" for v in valeurs.values()):
            largeur = max(valeurs) + 1
            lignes.append([valeurs.get(i, "") for i in range(largeur)])
    return lignes, feuille.get("name")


def nom_table(relatif):
    return snake_case(Path(relatif).stem)[:63]


def lister_fichiers(racine):
    """Fichiers de données / lexiques. Ignore PDF et fichiers verrou Office (~$...)."""
    fichiers = []
    for chemin in sorted(racine.rglob("*")):
        ext = chemin.suffix.lower()
        if not chemin.is_file() or chemin.name.startswith("~$") or ext not in (".csv", ".xlsx"):
            continue
        relatif = chemin.relative_to(racine).as_posix()
        fichiers.append(Fichier(chemin, relatif, ext[1:], nom_table(relatif)))
    # même nom de table pour deux fichiers (ex. .csv et .xlsx d'un même jeu) : suffixe le format
    compte = {}
    for f in fichiers:
        compte[f.table] = compte.get(f.table, 0) + 1
    for f in fichiers:
        if compte[f.table] > 1:
            f.table = f"{f.table[:58]}_{f.format}"
    return fichiers


def lire(f):
    octets = f.chemin.read_bytes()
    f.sha256 = hashlib.sha256(octets).hexdigest()
    if f.format == "csv":
        lignes, f.encodage, f.separateur = lire_csv(octets)
    else:
        lignes, f.feuille = lire_xlsx(octets)
    if not lignes:
        raise ValueError(f"{f.relatif} : fichier vide")
    f.colonnes_origine = lignes[0]
    largeur = max(len(l) for l in lignes)
    f.colonnes_origine += [""] * (largeur - len(f.colonnes_origine))
    f.colonnes = noms_colonnes(f.colonnes_origine)
    f.lignes = [l + [""] * (largeur - len(l)) for l in lignes[1:]]
    return f


def ident(nom):
    return '"' + nom.replace('"', '""') + '"'


def litteral(valeur):
    if valeur is None:
        return "NULL"
    return "'" + str(valeur).replace("'", "''") + "'"


def ddl_table(f):
    cols = ",\n".join(f"    {ident(c)} TEXT" for c in f.colonnes)
    return (
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{ident(f.table)} (\n"
        f"{cols},\n"
        "    _ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT now(),\n"
        "    _source TEXT NOT NULL,\n"
        "    _batch_id TEXT NOT NULL,\n"
        "    _fichier_origine TEXT NOT NULL,\n"
        "    _ligne INTEGER NOT NULL\n"
        ");\n"
    )


def script_chargement(f, batch_id):
    """Script psql d'un fichier : DDL, catalogue, COPY et contrôle du nombre de lignes."""
    table = f"{SCHEMA}.{ident(f.table)}"
    tampon = io.StringIO()
    ecrivain = csv.writer(tampon, lineterminator="\n")
    for n, ligne in enumerate(f.lignes, 2):
        ecrivain.writerow(ligne + [SOURCE, batch_id, f.relatif, n])
    donnees = tampon.getvalue()
    if any(l == "\\." for l in donnees.split("\n")):
        raise ValueError(f"{f.relatif} : ligne '\\.' incompatible avec COPY FROM STDIN")

    ajouts = "".join(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {ident(c)} TEXT;\n" for c in f.colonnes
    )
    colonnes_origine = "ARRAY[" + ",".join(litteral(c) for c in f.colonnes_origine) + "]::TEXT[]"
    liste = ", ".join(ident(c) for c in f.colonnes + ["_source", "_batch_id", "_fichier_origine", "_ligne"])
    return (
        ddl_table(f)
        + ajouts
        + f"INSERT INTO {SCHEMA}.ingestion_fichier (_batch_id, _source, _fichier_origine, sha256, "
        "taille_octets, format, encodage, separateur, feuille, table_cible, colonnes_origine, nb_lignes_lues)\n"
        f"VALUES ({litteral(batch_id)}, {litteral(SOURCE)}, {litteral(f.relatif)}, {litteral(f.sha256)}, "
        f"{f.chemin.stat().st_size}, {litteral(f.format)}, {litteral(f.encodage)}, {litteral(f.separateur)}, "
        f"{litteral(f.feuille)}, {litteral(f.table)}, {colonnes_origine}, {len(f.lignes)});\n"
        f"COPY {table} ({liste}) FROM STDIN WITH (FORMAT csv);\n"
        + donnees
        + "\\.\n"
        f"UPDATE {SCHEMA}.ingestion_fichier SET nb_lignes_ecrites = "
        f"(SELECT count(*) FROM {table} WHERE _batch_id = {litteral(batch_id)} "
        f"AND _fichier_origine = {litteral(f.relatif)}) "
        f"WHERE _batch_id = {litteral(batch_id)} AND _fichier_origine = {litteral(f.relatif)};\n"
        f"DO $$ BEGIN IF (SELECT nb_lignes_ecrites <> nb_lignes_lues FROM {SCHEMA}.ingestion_fichier "
        f"WHERE _batch_id = {litteral(batch_id)} AND _fichier_origine = {litteral(f.relatif)}) "
        "THEN RAISE EXCEPTION 'nombre de lignes écrites différent des lignes lues'; END IF; END $$;\n"
    )


def psql(script, *options):
    env = dict(os.environ, PGCLIENTENCODING="UTF8")
    for cle in ("HOST", "PORT", "DB", "USER", "PASSWORD"):
        valeur = os.environ.get(f"BRONZE_PG_{cle}")
        if valeur:
            env["PGDATABASE" if cle == "DB" else f"PG{cle}"] = valeur
    res = subprocess.run(
        ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", *options, "-f", "-"],
        input=script, text=True, encoding="utf-8", capture_output=True, env=env,
    )
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip())
    return res.stdout


def deja_ingeres():
    sortie = psql(f"SELECT _fichier_origine || '|' || sha256 FROM {SCHEMA}.ingestion_fichier;", "-At")
    return set(sortie.split())


def charger(racine):
    if not racine.is_dir():
        raise SystemExit(f"Dossier S3 introuvable : {racine} (voir S3_DATA_DIR)")
    psql(SCHEMA_SQL.read_text(encoding="utf-8"))
    connus = deja_ingeres()
    batch_id = uuid.uuid4().hex
    bilan = {"charges": 0, "ignores": 0, "lignes": 0}
    for f in lister_fichiers(racine):
        lire(f)
        if f"{f.relatif}|{f.sha256}" in connus:
            bilan["ignores"] += 1
            print(f"= {f.relatif} : déjà ingéré")
            continue
        psql(script_chargement(f, batch_id), "--single-transaction")
        bilan["charges"] += 1
        bilan["lignes"] += len(f.lignes)
        print(f"+ {f.relatif} -> {SCHEMA}.{f.table} : {len(f.lignes)} lignes, {len(f.colonnes)} colonnes")
    print(f"batch {batch_id} : {bilan['charges']} fichiers chargés ({bilan['lignes']} lignes), "
          f"{bilan['ignores']} déjà ingérés, 0 rejet")
    return bilan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("dossier", nargs="?", default=os.environ.get("S3_DATA_DIR", "data/Satisfaction"),
                        help="dossier Satisfaction (défaut : $S3_DATA_DIR ou data/Satisfaction)")
    parser.add_argument("--ddl", action="store_true", help="affiche le DDL des tables sans se connecter")
    args = parser.parse_args(argv)
    racine = Path(args.dossier)
    if args.ddl:
        print(SCHEMA_SQL.read_text(encoding="utf-8"))
        for f in lister_fichiers(racine):
            lire(f)
            print(f"-- {f.relatif}\n{ddl_table(f)}")
        return
    try:
        charger(racine)
    except RuntimeError as e:
        sys.exit(f"Échec du chargement : {e}")


if __name__ == "__main__":
    main()
