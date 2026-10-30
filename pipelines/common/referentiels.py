"""Référentiels géographiques : département déduit du code commune (puis du code postal), région (découpage 2016).

Fonctions pures sur des colonnes Spark, sans lecture ni écriture : elles se testent sans lac.
"""

from pyspark.sql import functions as F

from pipelines.common.normalize import normaliser_code_postal, normaliser_insee


def dictionnaire_departement():
    """Dictionnaire département -> libellé département, code région, libellé région.

    Source : INSEE, code officiel géographique (régions au 1er janvier 2016). Les collectivités d'outre-mer
    ont pour code région « COM », l'étranger « 99 » (annexe B.2 du livrable 1).
    """
    return {
        # "code département" : ("libellé département", "code région", "libellé région")
        "01": ("Ain", "84", "Auvergne-Rhône-Alpes"),
        "02": ("Aisne", "32", "Hauts-de-France"),
        "03": ("Allier", "84", "Auvergne-Rhône-Alpes"),
        "04": ("Alpes-de-Haute-Provence", "93", "Provence-Alpes-Côte d'Azur"),
        "05": ("Hautes-Alpes", "93", "Provence-Alpes-Côte d'Azur"),
        "06": ("Alpes-Maritimes", "93", "Provence-Alpes-Côte d'Azur"),
        "07": ("Ardèche", "84", "Auvergne-Rhône-Alpes"),
        "08": ("Ardennes", "44", "Grand Est"),
        "09": ("Ariège", "76", "Occitanie"),
        "10": ("Aube", "44", "Grand Est"),
        "11": ("Aude", "76", "Occitanie"),
        "12": ("Aveyron", "76", "Occitanie"),
        "13": ("Bouches-du-Rhône", "93", "Provence-Alpes-Côte d'Azur"),
        "14": ("Calvados", "28", "Normandie"),
        "15": ("Cantal", "84", "Auvergne-Rhône-Alpes"),
        "16": ("Charente", "75", "Nouvelle-Aquitaine"),
        "17": ("Charente-Maritime", "75", "Nouvelle-Aquitaine"),
        "18": ("Cher", "24", "Centre-Val de Loire"),
        "19": ("Corrèze", "75", "Nouvelle-Aquitaine"),
        "2A": ("Corse-du-Sud", "94", "Corse"),
        "2B": ("Haute-Corse", "94", "Corse"),
        "21": ("Côte-d'Or", "27", "Bourgogne-Franche-Comté"),
        "22": ("Côtes-d'Armor", "53", "Bretagne"),
        "23": ("Creuse", "75", "Nouvelle-Aquitaine"),
        "24": ("Dordogne", "75", "Nouvelle-Aquitaine"),
        "25": ("Doubs", "27", "Bourgogne-Franche-Comté"),
        "26": ("Drôme", "84", "Auvergne-Rhône-Alpes"),
        "27": ("Eure", "28", "Normandie"),
        "28": ("Eure-et-Loir", "24", "Centre-Val de Loire"),
        "29": ("Finistère", "53", "Bretagne"),
        "30": ("Gard", "76", "Occitanie"),
        "31": ("Haute-Garonne", "76", "Occitanie"),
        "32": ("Gers", "76", "Occitanie"),
        "33": ("Gironde", "75", "Nouvelle-Aquitaine"),
        "34": ("Hérault", "76", "Occitanie"),
        "35": ("Ille-et-Vilaine", "53", "Bretagne"),
        "36": ("Indre", "24", "Centre-Val de Loire"),
        "37": ("Indre-et-Loire", "24", "Centre-Val de Loire"),
        "38": ("Isère", "84", "Auvergne-Rhône-Alpes"),
        "39": ("Jura", "27", "Bourgogne-Franche-Comté"),
        "40": ("Landes", "75", "Nouvelle-Aquitaine"),
        "41": ("Loir-et-Cher", "24", "Centre-Val de Loire"),
        "42": ("Loire", "84", "Auvergne-Rhône-Alpes"),
        "43": ("Haute-Loire", "84", "Auvergne-Rhône-Alpes"),
        "44": ("Loire-Atlantique", "52", "Pays de la Loire"),
        "45": ("Loiret", "24", "Centre-Val de Loire"),
        "46": ("Lot", "76", "Occitanie"),
        "47": ("Lot-et-Garonne", "75", "Nouvelle-Aquitaine"),
        "48": ("Lozère", "76", "Occitanie"),
        "49": ("Maine-et-Loire", "52", "Pays de la Loire"),
        "50": ("Manche", "28", "Normandie"),
        "51": ("Marne", "44", "Grand Est"),
        "52": ("Haute-Marne", "44", "Grand Est"),
        "53": ("Mayenne", "52", "Pays de la Loire"),
        "54": ("Meurthe-et-Moselle", "44", "Grand Est"),
        "55": ("Meuse", "44", "Grand Est"),
        "56": ("Morbihan", "53", "Bretagne"),
        "57": ("Moselle", "44", "Grand Est"),
        "58": ("Nièvre", "27", "Bourgogne-Franche-Comté"),
        "59": ("Nord", "32", "Hauts-de-France"),
        "60": ("Oise", "32", "Hauts-de-France"),
        "61": ("Orne", "28", "Normandie"),
        "62": ("Pas-de-Calais", "32", "Hauts-de-France"),
        "63": ("Puy-de-Dôme", "84", "Auvergne-Rhône-Alpes"),
        "64": ("Pyrénées-Atlantiques", "75", "Nouvelle-Aquitaine"),
        "65": ("Hautes-Pyrénées", "76", "Occitanie"),
        "66": ("Pyrénées-Orientales", "76", "Occitanie"),
        "67": ("Bas-Rhin", "44", "Grand Est"),
        "68": ("Haut-Rhin", "44", "Grand Est"),
        "69": ("Rhône", "84", "Auvergne-Rhône-Alpes"),
        "70": ("Haute-Saône", "27", "Bourgogne-Franche-Comté"),
        "71": ("Saône-et-Loire", "27", "Bourgogne-Franche-Comté"),
        "72": ("Sarthe", "52", "Pays de la Loire"),
        "73": ("Savoie", "84", "Auvergne-Rhône-Alpes"),
        "74": ("Haute-Savoie", "84", "Auvergne-Rhône-Alpes"),
        "75": ("Paris", "11", "Île-de-France"),
        "76": ("Seine-Maritime", "28", "Normandie"),
        "77": ("Seine-et-Marne", "11", "Île-de-France"),
        "78": ("Yvelines", "11", "Île-de-France"),
        "79": ("Deux-Sèvres", "75", "Nouvelle-Aquitaine"),
        "80": ("Somme", "32", "Hauts-de-France"),
        "81": ("Tarn", "76", "Occitanie"),
        "82": ("Tarn-et-Garonne", "76", "Occitanie"),
        "83": ("Var", "93", "Provence-Alpes-Côte d'Azur"),
        "84": ("Vaucluse", "93", "Provence-Alpes-Côte d'Azur"),
        "85": ("Vendée", "52", "Pays de la Loire"),
        "86": ("Vienne", "75", "Nouvelle-Aquitaine"),
        "87": ("Haute-Vienne", "75", "Nouvelle-Aquitaine"),
        "88": ("Vosges", "44", "Grand Est"),
        "89": ("Yonne", "27", "Bourgogne-Franche-Comté"),
        "90": ("Territoire de Belfort", "27", "Bourgogne-Franche-Comté"),
        "91": ("Essonne", "11", "Île-de-France"),
        "92": ("Hauts-de-Seine", "11", "Île-de-France"),
        "93": ("Seine-Saint-Denis", "11", "Île-de-France"),
        "94": ("Val-de-Marne", "11", "Île-de-France"),
        "95": ("Val-d'Oise", "11", "Île-de-France"),
        "971": ("Guadeloupe", "01", "Guadeloupe"),
        "972": ("Martinique", "02", "Martinique"),
        "973": ("Guyane", "03", "Guyane"),
        "974": ("La Réunion", "04", "La Réunion"),
        "976": ("Mayotte", "06", "Mayotte"),
        "975": ("Saint-Pierre-et-Miquelon", "COM", "Collectivités d'outre-mer"),
        "977": ("Saint-Barthélemy", "COM", "Collectivités d'outre-mer"),
        "978": ("Saint-Martin", "COM", "Collectivités d'outre-mer"),
        "984": ("Terres australes et antarctiques françaises", "COM", "Collectivités d'outre-mer"),
        "986": ("Wallis-et-Futuna", "COM", "Collectivités d'outre-mer"),
        "987": ("Polynésie française", "COM", "Collectivités d'outre-mer"),
        "988": ("Nouvelle-Calédonie", "COM", "Collectivités d'outre-mer"),
        "989": ("Clipperton", "COM", "Collectivités d'outre-mer"),
        "99": ("Étranger", "99", "Étranger")
    }

# Département porté par les premiers caractères d'un code de 5 caractères : 3 pour l'outre-mer (97x, 98x), 2 sinon.
def _prefixe_departement(code):
    return F.when(code.substr(1, 2).isin("97", "98"), code.substr(1, 3)).otherwise(code.substr(1, 2))


# Garde le candidat seulement s'il est dans le dictionnaire : « 20 » ou « 00 » ne sont pas des départements.
def _departement_connu(candidat):
    return F.when(candidat.isin(*dictionnaire_departement()), candidat)


# Code postal : 20000 à 20199 en Corse-du-Sud, 20200 et au-delà en Haute-Corse ; ailleurs le préfixe suffit.
def _departement_depuis_postal(postal):
    corse = F.when(postal.substr(1, 3).isin("200", "201"), "2A").otherwise("2B")
    candidat = F.when(postal.substr(1, 2) == "20", corse).otherwise(_prefixe_departement(postal))
    return _departement_connu(candidat)


# Département déduit du code commune, à défaut du code postal. Renvoie la colonne du département.
def departement_de_commune(col_code_commune, col_code_postal):
    """Colonne département : code commune d'abord, code postal en repli, null si aucun des deux n'est exploitable.

    Les deux colonnes sont normalisées ici : sans cela « 1001 » (Ain, zéro perdu) serait lu comme le
    département 10 (Aube). Un code commune de format valide mais de département inconnu (les anciens codes
    corses 20xxx) passe lui aussi au code postal.
    """
    commune = normaliser_insee(col_code_commune)
    postal = normaliser_code_postal(col_code_postal)
    return F.coalesce(_departement_connu(_prefixe_departement(commune)), _departement_depuis_postal(postal))


# Attribut d'un département dans le dictionnaire (0 libellé département, 1 code région, 2 libellé région).
# F.create_map transforme le dictionnaire en une seule expression de recherche ; null si le département est inconnu.
def _attribut_departement(col_departement, position):
    paires = [F.lit(x) for code, valeurs in dictionnaire_departement().items() for x in (code, valeurs[position])]
    return F.element_at(F.create_map(*paires), col_departement)


def libelle_departement(col_departement):
    return _attribut_departement(col_departement, 0)


# Code région (découpage 2016) à partir du code département : 2 caractères en métropole, 3 en outre-mer.
def region_depuis_departement(col_departement):
    return _attribut_departement(col_departement, 1)


def libelle_region(col_departement):
    return _attribut_departement(col_departement, 2)
