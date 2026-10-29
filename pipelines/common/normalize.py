"""Normalisations communes aux jobs Silver : FINESS, INSEE, libellés de jointure, identifiants, clé de substitution.

Fonctions pures sur des colonnes Spark, sans lecture ni écriture : elles se testent sans lac.
Règles : docs/L1/Livrable_1_Big_Data_(final).pdf, annexe C.1 (Tableau 44) et annexe B.
"""

import unicodedata
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 9 chiffres, ou 2A / 2B suivi de 7 chiffres (Corse)
MOTIF_FINESS = r"^(\d{9}|2[AB]\d{7})$"
# 5 chiffres, ou 2A / 2B suivi de 3 chiffres (Corse)
MOTIF_INSEE = r"^(\d{5}|2[AB]\d{3})$"


def _code_normalise(col, longueur, motif):
    """Code nettoyé et complété de zéros à gauche jusqu'à `longueur`, ou null s'il est invalide.

    lpad tronque ce qui dépasse : on contrôle donc la longueur avant de compléter, sinon un code trop
    long deviendrait un code valide mais faux. Un texte vide ou fait de zéros n'est pas un code.
    """
    texte = F.upper(F.trim(col.cast("string")))
    complete = F.lpad(texte, longueur, "0")
    valide = (
        (F.length(texte) > 0)
        & (F.length(texte) <= longueur)
        & complete.rlike(motif)
        & ~complete.rlike("^0+$")
    )
    return F.when(valide, complete)  # sans otherwise : null quand le code est invalide


# FINESS sur 9 caractères, zéros initiaux restitués, null si invalide
def normaliser_finess(col_finess):
    return _code_normalise(col_finess, 9, MOTIF_FINESS)


# INSEE sur 5 caractères, zéros initiaux restitués, null si invalide (le code postal prendra alors le relais)
def normaliser_insee(col_insee):
    return _code_normalise(col_insee, 5, MOTIF_INSEE)


def _libelle_sans_accents(texte):
    """Majuscules, sans accents, espaces simples ; None si le texte est vide.

    upper() passe avant NFKD pour que « ß » devienne « SS ». NFKD sépare la lettre de son accent et
    remplace l'espace insécable par une espace. Limite : un caractère sans équivalent ASCII (œ, ’, –)
    est supprimé.
    """
    if texte is None:
        return None
    ascii_seul = unicodedata.normalize("NFKD", texte.upper()).encode("ASCII", "ignore").decode("ASCII")
    return " ".join(ascii_seul.split()) or None  # split() retire les espaces en trop et aux bords


# Libellé de jointure : majuscules, sans accents
def normaliser_libelle_jointure(col_libelle):
    return F.udf(_libelle_sans_accents, StringType())(col_libelle)


# Identifiant professionnel sans zéros initiaux ; null s'il est vide ou fait uniquement de zéros
def normaliser_identifiant_professionnel(col_identifiant):
    sans_zeros = F.regexp_replace(F.trim(col_identifiant), r"^0+", "")
    return F.when(sans_zeros != "", sans_zeros)


def cle_substitution(*colonnes):
    """Clé de substitution : abs(xxhash64(clé métier)), identique d'un chargement à l'autre.

    La clé métier doit être normalisée avant (casse, espaces, zéros), sinon deux écritures d'un même
    identifiant donnent deux clés. Les colonnes sont converties en texte pour que la clé ne dépende pas
    du type de la source. Null si une colonne est null : le fait pointera alors vers le membre -1.
    """
    connues = reduce(lambda a, b: a & b, [c.isNotNull() for c in colonnes])
    return F.when(connues, F.abs(F.xxhash64(*[c.cast("string") for c in colonnes])))
