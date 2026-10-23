"""EXTRACT des sources CSV (S2 établissements / professionnels, Hospitalisations) vers Bronze.

    python -m pipelines.bronze.csv_bronze                              # les 4 fichiers
    python -m pipelines.bronze.csv_bronze --table s2_etablissement_sante

Aucune transformation : toutes les colonnes restent du texte, on ajoute seulement les colonnes
techniques. Relancer le job sur un fichier de même contenu ne fait rien ; une nouvelle version
s'ajoute à côté de l'ancienne (Silver dédoublonnera).

Variables d'environnement :
    DATA_DIR    dossier des CSV, « data » par défaut
    LAKE_ROOT   racine du lac, « s3a:// » par défaut ; tant que le stockage S3 n'est pas branché
                (aucune configuration S3A ici), pointer vers un dossier local

Une ligne dont la structure est cassée (trop ou pas assez de champs) va dans
<LAKE_ROOT>/silver/_rejets/<source>, avant l'écriture Bronze : après une panne suivie d'une
reprise, un rejet peut donc être écrit deux fois (le _batch_id permet de le dédoublonner).
Chaque exécution ajoute une ligne (lues / écrites / rejetées) à <LAKE_ROOT>/bronze/_journal.
"""

import argparse
import csv
import hashlib
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

log = logging.getLogger("csv_bronze")

JOB = "csv_bronze"
SEPARATEUR = ";"
LIGNE_BRUTE = "_ligne_brute"
TAILLE_BLOC = 1024 * 1024
COLONNES_TECHNIQUES = ("_ingestion_ts", "_source", "_batch_id", "_fichier_origine")

SCHEMA_JOURNAL = StructType([
    StructField("job", StringType()),
    StructField("source", StringType()),
    StructField("table", StringType()),
    StructField("batch_id", StringType()),
    StructField("lignes_lues", LongType()),
    StructField("lignes_ecrites", LongType()),
    StructField("lignes_rejetees", LongType()),
    StructField("statut", StringType()),
    StructField("message", StringType()),
    StructField("debut", TimestampType()),
    StructField("fin", TimestampType()),
])

# Définition du chemin cible
def chemin_lake(*parties):
    racine = os.environ.get("LAKE_ROOT") or "s3a://"
    separateur = "" if racine.endswith("/") else "/"
    return racine + separateur + "/".join(parties)

@dataclass(frozen=True)
class SourceCsv:
    source: str
    table: str
    dossier: str
    fichier: str
    colonnes: tuple
    encodage: str = "UTF-8"

    @property
    def chemin_delta(self):
        return chemin_lake("bronze", self.source, self.table.removeprefix(f"{self.source}_"))


@dataclass
class Compteurs:
    lues: int = 0
    ecrites: int = 0
    rejetees: int = 0


# Les colonnes doivent rester identiques à pipelines/bronze/CSV_schema.sql
CATALOGUE = (
    SourceCsv(
        source="s2",
        table="s2_etablissement_sante",
        dossier="Etablissement de SANTE",
        fichier="etablissement_sante.csv",
        colonnes=(
            "adresse", "cedex", "code_commune", "code_postal", "commune",
            "complement_destinataire", "complement_point_geographique", "email",
            "enseigne_commerciale_site", "finess_etablissement_juridique", "finess_site",
            "identifiant_organisation", "indice_repetition_voie", "mention_distribution",
            "numero_voie", "pays", "raison_sociale_site", "siren_site", "siret_site",
            "telecopie", "telephone", "telephone_2", "type_voie", "voie",
        ),
    ),
    SourceCsv(
        source="s2",
        table="s2_professionnel_sante",
        dossier="Etablissement de SANTE",
        fichier="professionnel_sante.csv",
        colonnes=(
            "identifiant", "civilite", "categorie_professionnelle", "nom", "prenom",
            "commune", "profession", "specialite", "type_identifiant",
        ),
        # mélange d'UTF-8 et de Windows-1252 : Latin-1 lit n'importe quel octet sans rien perdre
        encodage="ISO-8859-1",
    ),
    SourceCsv(
        source="s2",
        table="s2_activite_professionnel_sante",
        dossier="Etablissement de SANTE",
        fichier="activite_professionnel_sante.csv",
        colonnes=("identifiant", "identifiant_organisation", "mode_exercice"),
    ),
    SourceCsv(
        source="hosp",
        table="hosp_hospitalisations",
        dossier="Hospitalisation",
        fichier="Hospitalisations.csv",
        colonnes=(
            "num_hospitalisation", "id_patient", "identifiant_organisation", "code_diagnostic",
            "suite_diagnostic_consultation", "date_entree", "jour_hospitalisation",
        ),
    ),
)

# INGESTION________________________________________________________________________________________________

# creer_session permet de créer une session Spark avec les extensions Delta Lake activées. 
# DELTA_JARS_INCLUS à "1" configure Spark avec les bibliothèques Delta Lake via pip.
def creer_session():
    builder = (
        SparkSession.builder.appName(JOB)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.enabled", "false")
    )
    if os.environ.get("DELTA_JARS_INCLUS") != "1":
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()


def calculer_batch_id(chemin):
    """Empreinte du contenu du fichier : le même fichier redonne toujours le même lot."""
    sha = hashlib.sha256()
    with open(chemin, "rb") as f:
        for bloc in iter(lambda: f.read(TAILLE_BLOC), b""):
            sha.update(bloc)
    return sha.hexdigest()[:12]


def lot_deja_ingere(spark, chemin_table, batch_id):
    """Vrai si la table Delta de `chemin_table` contient déjà des lignes de ce lot."""
    if not DeltaTable.isDeltaTable(spark, chemin_table):
        return False
    lot = spark.read.format("delta").load(chemin_table).filter(F.col("_batch_id") == batch_id)
    
    return not lot.isEmpty()


def ajouter_delta(spark, df, chemin):
    """Ajoute `df` à la table Delta de `chemin`, créée en append-only si elle n'existe pas."""
    (
        DeltaTable.createIfNotExists(spark)
        .location(chemin)
        .addColumns(df.schema)
        .property("delta.appendOnly", "true")
        .execute()
    )
    df.write.format("delta").mode("append").save(chemin)


def verifier_entete(chemin_fichier, cfg):
    """Échoue tout de suite si la source a changé de format (noms ou ordre des colonnes)."""
    with open(chemin_fichier, "rb") as f:
        ligne = f.readline().decode(cfg.encodage).lstrip("\ufeff")
    trouvees = [c.strip().lower() for c in next(csv.reader([ligne], delimiter=SEPARATEUR))]
    if trouvees != list(cfg.colonnes):
        raise ValueError(
            f"En-tête inattendu dans {cfg.fichier} : attendu {list(cfg.colonnes)}, trouvé {trouvees}"
        )


def lire_csv(spark, chemin_fichier, cfg):
    schema = StructType(
        [StructField(c, StringType()) for c in cfg.colonnes] + [StructField(LIGNE_BRUTE, StringType())]
    )
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("sep", SEPARATEUR)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", cfg.encodage)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", LIGNE_BRUTE)
        .csv(str(chemin_fichier))
    )


def ecrire_rejets(spark, rejetees, cfg, batch_id, ingestion_ts):
    rejets = rejetees.select(
        F.lit("ligne_malformee").alias("motif"),
        F.lit(ingestion_ts).alias("_ingestion_ts"),
        F.lit(JOB).alias("job"),
        F.lit(cfg.fichier).alias("_fichier_origine"),
        F.lit(batch_id).alias("_batch_id"),
        F.col(LIGNE_BRUTE).alias("ligne_origine"),
    )
    ajouter_delta(spark, rejets, chemin_lake("silver", "_rejets", cfg.source))

# charger lit le CSV et l'écrit dans la table Delta, en journalisant les compteurs. Échoue si le nombre de lignes écrites ne correspond pas à l'attendu.
def charger(spark, cfg, chemin_fichier, batch_id, compteurs, ingestion_ts):
    df = lire_csv(spark, chemin_fichier, cfg).persist(StorageLevel.MEMORY_AND_DISK) # on persiste pour éviter de relire le CSV plusieurs fois
    try:
        cassee = F.col(LIGNE_BRUTE).isNotNull()
        rejetees = df.filter(cassee)
        compteurs.lues = df.count()
        compteurs.rejetees = rejetees.count()
        if compteurs.rejetees:
            ecrire_rejets(spark, rejetees, cfg, batch_id, ingestion_ts)
        propres = (
            df.filter(~cassee).drop(LIGNE_BRUTE)
            .withColumn("_ingestion_ts", F.lit(ingestion_ts))
            .withColumn("_source", F.lit(cfg.source))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_fichier_origine", F.lit(cfg.fichier))
        )
        ajouter_delta(spark, propres, cfg.chemin_delta)
        compteurs.ecrites = (
            spark.read.format("delta").load(cfg.chemin_delta).filter(F.col("_batch_id") == batch_id).count()
        )
    finally:
        df.unpersist()
    if compteurs.ecrites != compteurs.lues - compteurs.rejetees:
        raise RuntimeError(
            f"{cfg.table} : {compteurs.lues} lues, {compteurs.rejetees} rejetées, "
            f"mais {compteurs.ecrites} écrites dans Bronze"
        )


def ecrire_journal(spark, cfg, batch_id, compteurs, statut, debut, message):
    fin = datetime.now(timezone.utc)
    log.info(
        "%s statut=%s lues=%d ecrites=%d rejetees=%d",
        cfg.table, statut, compteurs.lues, compteurs.ecrites, compteurs.rejetees,
    )
    ligne = (JOB, cfg.source, cfg.table, batch_id, compteurs.lues, compteurs.ecrites,
             compteurs.rejetees, statut, message, debut, fin)
    ajouter_delta(spark, spark.createDataFrame([ligne], SCHEMA_JOURNAL), chemin_lake("bronze", "_journal"))


# ingerer_csv ingère un fichier dans sa table Bronze et journalise l'exécution. Renvoie le statut.
def ingerer_csv(spark, cfg, chemin_fichier):
    """Ingère un fichier dans sa table Bronze et journalise l'exécution. Renvoie le statut."""
    debut = datetime.now(timezone.utc)
    compteurs = Compteurs()
    batch_id, statut, message = None, "ok", None
    try:
        verifier_entete(chemin_fichier, cfg) # vérifie que la source n'a pas changé de format (noms ou ordre des colonnes)
        batch_id = calculer_batch_id(chemin_fichier) # identifiant unique du lot, basé sur le contenu du fichier
        if lot_deja_ingere(spark, cfg.chemin_delta, batch_id): # si la table Delta contient déjà des lignes de ce lot, on ne fait rien
            statut = "ignore"
        else: # sinon, on lit le CSV et on l'écrit dans la table Delta
            charger(spark, cfg, chemin_fichier, batch_id, compteurs, debut) # lit le CSV et l'écrit dans la table Delta
    except Exception as e:
        statut, message = "echec", str(e)[:500]
        raise
    finally:
        try:
            ecrire_journal(spark, cfg, batch_id, compteurs, statut, debut, message)
        except Exception:
            log.exception("Journal non écrit pour %s", cfg.table)
    return statut


def main(argv=None):
    tables = [c.table for c in CATALOGUE]
    parser = argparse.ArgumentParser(description="Ingestion des CSV S2 et Hospitalisations vers Bronze")
    parser.add_argument("--table", action="append", choices=tables, help="table à ingérer (défaut : toutes)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    donnees = Path(os.environ.get("DATA_DIR", "data"))
    spark = creer_session()
    echecs = 0
    for cfg in CATALOGUE:
        if args.table and cfg.table not in args.table:
            continue
        try:
            ingerer_csv(spark, cfg, donnees / cfg.dossier / cfg.fichier)
        except Exception:
            log.exception("Échec de l'ingestion de %s", cfg.table)
            echecs += 1
    return 1 if echecs else 0


if __name__ == "__main__":
    sys.exit(main())
