"""Lecture et écriture Delta : dernier lot Bronze, ajout, dédoublonnage sur la clé métier, MERGE Silver.

dedoublonner, fusionner et lire_dernier_lot s'appuient sur _ingestion_ts et _batch_id, colonnes techniques de
Bronze à conserver en Silver.
Règles : CLAUDE.md §6 (doublons, chargement) et docs/L1/Livrable_1_Big_Data_(final).pdf, annexe C.1.
"""

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F


def ajouter_delta(spark, df, chemin, evolution_schema=False):
    """Ajoute `df` à la table Delta de `chemin`, créée en append-only si elle n'existe pas.

    evolution_schema=True laisse la table gagner les colonnes qui lui manquent (journal : une colonne ajoutée
    ne doit pas casser un lac déjà alimenté).
    """
    # Création seulement si la table n'existe pas : sur une table existante, seul l'ajout (et son évolution) compte
    if not DeltaTable.isDeltaTable(spark, chemin):
        (
            DeltaTable.create(spark)
            .location(chemin)
            .addColumns(df.schema)
            .property("delta.appendOnly", "true")
            .execute()
        )
    df.write.format("delta").mode("append").option("mergeSchema", str(evolution_schema).lower()).save(chemin)


def lire_dernier_lot(spark, chemin):
    """Lignes du dernier lot de la table Bronze de `chemin` (_ingestion_ts le plus récent). Renvoie (df, batch_id).

    Bronze ajoute une version à côté de l'ancienne quand le fichier change : lire tout compterait chaque
    ligne de l'ancienne version comme un doublon.
    """
    table = spark.read.format("delta").load(chemin)
    dernier = table.orderBy(F.col("_ingestion_ts").desc(), F.col("_batch_id").desc()).select("_batch_id").first()
    if dernier is None:
        raise ValueError(f"Aucun lot dans {chemin}")
    return table.filter(F.col("_batch_id") == dernier["_batch_id"]), dernier["_batch_id"]


# Une clé peut être donnée comme un nom de colonne ou comme une liste de noms
def _en_liste(cles):
    return [cles] if isinstance(cles, str) else list(cles)

# Garde une seule ligne par clé métier, la plus récente. Renvoie (lignes uniques, nombre de lignes écartées).
def dedoublonner(df, cles):
    """Garde une seule ligne par clé métier, la plus récente. Renvoie (lignes uniques, nombre de lignes écartées).

    À égalité de _ingestion_ts, le hash de la ligne départage : le choix est arbitraire mais stable, sinon deux
    exécutions pourraient garder deux lignes différentes. Les clés nulles doivent avoir été rejetées avant :
    elles formeraient un seul groupe.
    """
    # fenetre comprend toutes les colonnes de df pour que le hash dépende de la ligne entière, pas seulement des clés
    fenetre = Window.partitionBy(*_en_liste(cles)).orderBy(
        F.col("_ingestion_ts").desc(), F.xxhash64(*df.columns).desc()
    )
    uniques = df.withColumn("_rang", F.row_number().over(fenetre)).filter(F.col("_rang") == 1).drop("_rang") # garde la ligne la plus récente par clé, départage à égalité par hash de la ligne entière
    return uniques, df.count() - uniques.count() # uniques correspond à la table dédoublonnée, le reste est rejeté

# Fusionne lorsque la table existe déjà, ou crée la table sinon. La table est mise à jour seulement si la ligne source est plus récente que la ligne cible.
def fusionner(spark, df, chemin, cles):
    """Charge `df` dans la table Delta de `chemin` par MERGE sur `cles` ; la table est créée si elle n'existe pas.

    `df` doit avoir une seule ligne par clé (voir dedoublonner) : si une même ligne de la table correspond à
    deux lignes de `df`, Delta refuse le MERGE. Sur une table vide il ne détecte rien et insère tout.
    Une ligne n'est mise à jour que si elle est plus récente que celle de la table : rejouer un lot ne change
    rien, et un vieux lot n'écrase pas un lot récent.

    Renvoie (lignes insérées, lignes mises à jour), pour le journal.
    """
    # Pas de delta.appendOnly : une table append-only refuserait les mises à jour du MERGE
    (
        DeltaTable.createIfNotExists(spark)
        .location(chemin)
        .addColumns(df.schema)
        .execute()
    )
    condition = " AND ".join(f"cible.{c} = source.{c}" for c in _en_liste(cles))
    table = DeltaTable.forPath(spark, chemin)
    version_avant = table.history(1).first()["version"]
    (
        table.alias("cible")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll(condition="source._ingestion_ts > cible._ingestion_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )
    # Un MERGE qui ne change rien peut ne laisser aucun commit : history(1) serait alors le commit précédent
    dernier = table.history(1).first()
    if dernier["version"] == version_avant or dernier["operation"] != "MERGE":
        return 0, 0
    metriques = dernier["operationMetrics"]
    return int(metriques["numTargetRowsInserted"]), int(metriques["numTargetRowsUpdated"])
