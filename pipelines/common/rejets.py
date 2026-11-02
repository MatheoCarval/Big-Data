"""Rejets Silver : table Delta <LAKE_ROOT>/silver/_rejets/<source>.

Même schéma que les rejets Bronze (lignes malformées), pour que tous les rejets d'une source soient au même endroit :
    motif, _ingestion_ts, job, _fichier_origine, _batch_id, ligne_origine
`ligne_origine` est la ligne Bronze en JSON (livrable 1, annexe C.2), limitée aux colonnes que le job autorise :
Silver ne contient aucune donnée directement identifiante (CLAUDE.md §7.2), même dans ses rejets.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pipelines.common.delta_io import ajouter_delta
from pipelines.common.session_spark import chemin_lake

CLE_REJET = ["job", "_batch_id", "motif", "ligne_origine"]


def ecrire_rejets(spark, rejetees, source, job, ingestion_ts, colonnes_json):
    """Ajoute à silver/_rejets/<source> les lignes Bronze `rejetees` (colonnes d'origine + colonne `motif`).

    `colonnes_json` : les seules colonnes d'origine recopiées dans `ligne_origine` (liste blanche, jamais adresse,
    e-mail, téléphone, identifiant patient...). Les colonnes nulles sont omises du JSON.
    Rejouer le job sur le même lot n'ajoute rien : un rejet déjà écrit (même job, lot, motif et ligne) est ignoré.
    """
    chemin = chemin_lake("silver", "_rejets", source)
    rejets = rejetees.select(
        F.col("motif"),
        F.lit(ingestion_ts).alias("_ingestion_ts"),
        F.lit(job).alias("job"),
        F.col("_fichier_origine"),
        F.col("_batch_id"),
        F.to_json(F.struct(*colonnes_json)).alias("ligne_origine"),
    )
    if DeltaTable.isDeltaTable(spark, chemin):
        deja = spark.read.format("delta").load(chemin).select(*CLE_REJET)
        rejets = rejets.join(deja, CLE_REJET, "left_anti")
    ajouter_delta(spark, rejets, chemin)
