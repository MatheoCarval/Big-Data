"""TRANSFORM de S2 établissements : dernier lot Bronze → Silver `etablissement`.

    python -m pipelines.silver.etablissement

Lit le dernier lot de <LAKE_ROOT>/bronze/s2/etablissement_sante (Bronze seulement), écarte les rejets vers
<LAKE_ROOT>/silver/_rejets/s2, dédoublonne sur identifiant_organisation puis fusionne (MERGE) dans
<LAKE_ROOT>/silver/etablissement. Chaque exécution ajoute une ligne à <LAKE_ROOT>/silver/_journal.

Variables d'environnement : LAKE_ROOT, MINIO_* (voir pipelines/common/session_spark.py).
Spécification : docs/L1/Livrable_1_Big_Data_(final).pdf, annexe C.2 (Tableau 46) et annexe B.3.
"""

import logging
import sys
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import functions as F # F permet d'utiliser les fonctions Spark SQL comme F.col, F.lit, F.when, ...

from pipelines.common.delta_io import dedoublonner, fusionner, lire_dernier_lot
from pipelines.common.journal import Compteurs, ecrire_journal
from pipelines.common.normalize import (
    normaliser_code_postal,
    normaliser_finess,
    normaliser_identifiant_organisation,
    normaliser_insee,
    normaliser_libelle_jointure,
)
from pipelines.common.referentiels import (
    departement_de_commune,
    libelle_region,
    region_depuis_departement,
)
from pipelines.common.rejets import ecrire_rejets
from pipelines.common.session_spark import chemin_lake, creer_session

log = logging.getLogger("silver_etablissement")

JOB = "silver_etablissement"
SOURCE = "s2"
TABLE = "etablissement"
CLE = "identifiant_organisation"

# Colonnes de la table Silver, dans cet ordre. Ni adresse, ni e-mail, ni téléphone, ni télécopie :
# on ne garde que ce qui alimente dim_etablissement et dim_localisation.
COLONNES_SILVER = (
    "identifiant_organisation",
    "finess_site",
    "finess_juridique",
    "raison_sociale",
    "type_etablissement",
    "commune",
    "code_commune",
    "code_postal",
    "departement",
    "code_region",
    "libelle_region",
    "_ingestion_ts",
    "_source",
    "_batch_id",
    "_fichier_origine",
)

# Colonnes Bronze recopiées dans le JSON d'un rejet (liste blanche) : pas d'adresse, d'e-mail ni de téléphone
COLONNES_REJET = (
    "identifiant_organisation",
    "finess_site",
    "finess_etablissement_juridique",
    "raison_sociale_site",
    "code_commune",
    "commune",
    "code_postal",
    "pays",
)

# Règles du type d'établissement : (libellé, motif), de la plus spécifique à la plus générale, la première qui
# correspond gagne ; "Autre" est le cas par défaut. Les motifs s'appliquent à la raison sociale normalisée
# (majuscules, sans accents). Les sigles courts (CH, MAS, SELARL...) sont ancrés en début de libellé (^) : plus
# loin dans le nom ils désignent autre chose (« CABINET CH DUPONT » n'est pas un centre hospitalier).
REGLES_TYPE = (
    ("CHU / régional", r"\bCHU\b|\bCHRU?\b|\bCENTRE HOSPITALIER (UNIVERSITAIRE|REGIONAL)\b"),
    ("Centre hospitalier", r"\bCENTRE HOSPITALIER\b|^CH\b|^HOPITAL\b|^GROUPE HOSPITALIER\b"),
    ("Clinique", r"\bCLINIQUE\b"),
    ("Pharmacie", r"\bPHARMACIE\b"),
    ("Laboratoire", r"\bLABORATOIRE\b|^(LBM|LABO)\b"),
    ("Imagerie", r"\bIMAGERIE\b|\bRADIOLOGIE\b|\bSCANNER\b|\bIRM\b"),
    ("Médico-social", r"\bEHPAD\b|\bMAISON DE RETRAITE\b|\bFOYER DE VIE\b|\bFOYER D'ACCUEIL\b|^(SSIAD|SESSAD|IME|ESAT|CMPP|MAS|FAM|FOYER)\b"),
    ("Centre de santé", r"\bCENTRE DE SANTE\b|^(CDS|CTRE DE SANTE|CENTRE MEDICAL)\b"),
    ("Cabinet libéral", r"\bCABINET\b|^(SELARL|SELAS|SCP|SEL|CAB|DOCTEUR|DR)\b"),
)


def transformer(bronze):
    """Sépare les lignes du lot Bronze en (valides, rejetees).

    valides  : une ligne par ligne Bronze valide, avec exactement les colonnes de COLONNES_SILVER, dans cet ordre
    rejetees : les lignes Bronze invalides, colonnes d'origine inchangées + une colonne `motif`

    Chaque ligne Bronze doit se retrouver dans l'un des deux (le job vérifie que lues = valides + rejetées).
    Le détail des règles est dans la fiche donnée avec cette étape.
    """

    # --- A. Séparer valides et rejetées (aucune ligne ne doit disparaître) ---
    # Calculer l'identifiant normalisé
    identifiant_normalise = normaliser_identifiant_organisation(F.col("identifiant_organisation")) # F.col("x") désigne la colonne x sans lire de données : identifiant_normalise est une expression, calculée plus tard par Spark.
    # rejetees : les lignes de bronze dont cet identifiant est nul
    rejetees = bronze.filter(identifiant_normalise.isNull()).withColumn("motif", F.lit("identifiant_organisation_manquant"))
    # valides : les lignes de bronze dont cet identifiant n'est pas nul
    valides = bronze.filter(identifiant_normalise.isNotNull())

    # --- B. Les codes : nuls s'ils sont invalides ---
    # Remplacer la colonne "identifiant_organisation" par l'identifiant normalisé
    valides = valides.withColumn("identifiant_organisation", identifiant_normalise) # withColumn(nom, expression) renvoie un nouveau DataFrame (ils sont immuables, d'où `valides = valides...`) où la colonne `nom` est créée ou remplacée.
    # Colonne "finess_site" : normaliser_finess
    valides = valides.withColumn("finess_site", normaliser_finess(F.col("finess_site")))
    # Nouvelle colonne "finess_juridique"
    valides = valides.withColumn("finess_juridique", normaliser_finess(F.col("finess_etablissement_juridique")))
    # Colonne "code_commune"
    valides = valides.withColumn("code_commune", normaliser_insee(F.col("code_commune")))
    # Colonne "code_postal"
    valides = valides.withColumn("code_postal", normaliser_code_postal(F.col("code_postal")))

    # --- C. La géographie : "NR" ou "Non renseignée" quand elle est inconnue ---
    # Variable Python dep
    dep = departement_de_commune(F.col("code_commune"), F.col("code_postal"))
    # Colonne "code_region" 
    valides = valides.withColumn("code_region", F.coalesce(region_depuis_departement(dep), F.lit("NR")))
    # Colonne "libelle_region"
    valides = valides.withColumn("libelle_region", F.coalesce(libelle_region(dep), F.lit("Non renseigné")))
    # Colonne "departement" 
    valides = valides.withColumn("departement", F.coalesce(dep, F.lit("NR")))

    # --- D. Les libellés : "Non renseigné" quand ils sont vides ---
    # Colonne de travail "libelle_raison"
    valides = valides.withColumn("libelle_raison", normaliser_libelle_jointure(F.col("raison_sociale_site")))
    # Colonne "raison_sociale"
    valides = valides.withColumn("raison_sociale", F.coalesce(F.col("libelle_raison"), F.lit("Non renseigné")))
    # Colonne "commune"
    valides = valides.withColumn("commune", F.coalesce(normaliser_libelle_jointure(F.col("commune")), F.lit("Non renseigné")))

    # --- E. Le type d'établissement ---
    # Variable type_etab
    type_etab = F.when(F.col("libelle_raison").rlike(REGLES_TYPE[0][1]), REGLES_TYPE[0][0]) # F.when crée une condition : si la colonne "libelle_raison" correspond au motif de la première règle, alors type_etab prend le libellé correspondant.
    # Boucle sur le reste de REGLES_TYPE
    for libelle, motif in REGLES_TYPE[1:]:
        type_etab = type_etab.when(F.col("libelle_raison").rlike(motif), libelle)
    # Colonne "type_etablissement" : type_etab.otherwise("Autre")
    valides = valides.withColumn("type_etablissement", type_etab.otherwise("Autre"))

    # --- F. La sélection finale ---
    # Ne garder que les colonnes de COLONNES_SILVER, dans cet ordre : .select(*COLONNES_SILVER)
    # ("libelle_raison", l'adresse, l'e-mail et le téléphone disparaissent ici)
    valides = valides.select(*COLONNES_SILVER)
    # return valides, rejetees
    return valides, rejetees


def verifier_schema(valides):
    if valides.columns != list(COLONNES_SILVER):
        raise ValueError(f"transformer doit renvoyer les colonnes {list(COLONNES_SILVER)}, pas {valides.columns}")


def executer(spark):
    """Exécute le job et journalise l'exécution. Renvoie le statut."""
    debut = datetime.now(timezone.utc)
    compteurs = Compteurs()
    batch_id, statut, message = None, "ok", None
    en_cache = []  # DataFrames persistés, libérés à la fin
    try:
        bronze, batch_id = lire_dernier_lot(spark, chemin_lake("bronze", SOURCE, "etablissement_sante")) # lire_dernier_lot lit la table Delta Bronze et ne garde que les lignes du lot le plus récent (_ingestion_ts) ; renvoie ce DataFrame et son batch_id.
        bronze = bronze.persist(StorageLevel.MEMORY_AND_DISK)
        en_cache.append(bronze)
        compteurs.lues = bronze.count()

        valides, rejetees = transformer(bronze)
        verifier_schema(valides)
        valides = valides.persist(StorageLevel.MEMORY_AND_DISK)
        en_cache.append(valides)
        compteurs.rejetees = rejetees.count()
        nb_valides = valides.count()
        if compteurs.lues != compteurs.rejetees + nb_valides:
            raise RuntimeError(
                f"{compteurs.lues} lignes lues, mais {nb_valides} valides et {compteurs.rejetees} rejetées : "
                "transformer a perdu ou dupliqué des lignes"
            )
        if compteurs.rejetees:
            ecrire_rejets(spark, rejetees, SOURCE, JOB, debut, COLONNES_REJET)

        uniques, compteurs.dedoublonnees = dedoublonner(valides, CLE)
        inserees, mises_a_jour = fusionner(spark, uniques, chemin_lake("silver", TABLE), CLE)
        compteurs.ecrites = inserees + mises_a_jour
    except Exception as e:
        statut, message = "echec", str(e)[:500]
        raise
    finally:
        for df in en_cache:
            df.unpersist()
        try:
            ecrire_journal(spark, "silver", JOB, SOURCE, TABLE, batch_id, compteurs, statut, debut, message)
        except Exception:
            log.exception("Journal non écrit pour %s", TABLE)
    return statut


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = creer_session(JOB)
    try:
        executer(spark)
    except Exception:
        log.exception("Échec de %s", JOB)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
