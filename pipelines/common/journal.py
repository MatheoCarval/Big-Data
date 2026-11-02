"""Journal d'exécution des jobs : une ligne par exécution, table Delta <LAKE_ROOT>/<couche>/_journal.

Même schéma pour Bronze et Silver (livrable 1, annexe C.2 : lignes lues, écrites, rejetées, doublons, durée, statut).
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from pipelines.common.delta_io import ajouter_delta
from pipelines.common.session_spark import chemin_lake

log = logging.getLogger("journal")

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
    # en dernier : les journaux Bronze déjà écrits ne l'ont pas, la table l'ajoute au prochain ajout
    StructField("lignes_dedoublonnees", LongType()),
])


@dataclass
class Compteurs:
    lues: int = 0
    ecrites: int = 0
    rejetees: int = 0
    dedoublonnees: int = 0


def ecrire_journal(spark, couche, job, source, table, batch_id, compteurs, statut, debut, message=None):
    """Ajoute la ligne de cette exécution à <LAKE_ROOT>/<couche>/_journal."""
    fin = datetime.now(timezone.utc)
    log.info(
        "%s statut=%s lues=%d ecrites=%d rejetees=%d dedoublonnees=%d",
        table, statut, compteurs.lues, compteurs.ecrites, compteurs.rejetees, compteurs.dedoublonnees,
    )
    ligne = (job, source, table, batch_id, compteurs.lues, compteurs.ecrites, compteurs.rejetees,
             statut, message, debut, fin, compteurs.dedoublonnees)
    ajouter_delta(
        spark, spark.createDataFrame([ligne], SCHEMA_JOURNAL), chemin_lake(couche, "_journal"), evolution_schema=True
    )
