"""Normalisations communes aux jobs Silver : FINESS, INSEE, libellés de jointure, identifiants, clé de substitution.

Fonctions pures sur des colonnes Spark, sans lecture ni écriture : elles se testent sans lac.
"""

import unicodedata
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# DEFINITION DES NORMES ========================================================================================

# 9 chiffres, ou 2A / 2B suivi de 7 chiffres (Corse)
MOTIF_FINESS = r"^(\d{9}|2[AB]\d{7})$"
# 5 chiffres, ou 2A / 2B suivi de 3 chiffres (Corse)
MOTIF_INSEE = r"^(\d{5}|2[AB]\d{3})$"
# 5 chiffres (la Corse a des codes postaux 20xxx, pas de lettre)
MOTIF_CODE_POSTAL = r"^\d{5}$"


# FONCTIONS NORMALISATION ======================================================================================

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


# Code postal sur 5 chiffres, zéros initiaux restitués, null si invalide
def normaliser_code_postal(col_code_postal):
    return _code_normalise(col_code_postal, 5, MOTIF_CODE_POSTAL)


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
    sans_zeros = F.regexp_replace(F.trim(col_identifiant), r"^0+", "")# Retirer les zéros initiaux
    return F.when(sans_zeros != "", sans_zeros)

# Calcule la clé de substitution à partir des colonnes identifiants initiales, après normalisation. 
# La clé est identique d'un chargement à l'autre, même si les colonnes changent de type.
def cle_substitution(*colonnes):
    """Clé de substitution : abs(xxhash64(clé métier)), identique d'un chargement à l'autre.

    La clé métier doit être normalisée avant (casse, espaces, zéros), sinon deux écritures d'un même
    identifiant donnent deux clés. Les colonnes sont converties en texte pour que la clé ne dépende pas
    du type de la source. Null si une colonne est null : le fait pointera alors vers le membre -1.

    xxhash64 est un hash rapide qui produit un bigint signé : abs() le rend positif, ce qui garde -1
    réservé au membre « Non renseigné ».
    """
    connues = reduce(lambda a, b: a & b, [c.isNotNull() for c in colonnes]) # toutes les colonnes doivent être non nulles pour calculer la clé
    return F.when(connues, F.abs(F.xxhash64(*[c.cast("string") for c in colonnes])))