

CREATE SCHEMA IF NOT EXISTS bronze_csv;

-- 3 tables correspondants à la source 2 
CREATE TABLE IF NOT EXISTS bronze_csv.etablissement_sante (
    adresse TEXT,
    cedex TEXT,
    code_commune TEXT,
    code_postal TEXT,
    commune TEXT,
    complement_destinataire TEXT,
    complement_point_geographique TEXT,
    email TEXT,
    enseigne_commerciale_site TEXT,
    finess_etablissement_juridique TEXT,
    finess_site TEXT,
    identifiant_organisation TEXT,
    indice_repetition_voie TEXT,
    mention_distribution TEXT,
    numero_voie TEXT,
    pays TEXT,
    raison_sociale_site TEXT,
    siren_site TEXT,
    siret_site TEXT,
    telecopie TEXT,
    telephone TEXT,
    telephone_2 TEXT,
    type_voie TEXT,
    voie TEXT
);

CREATE TABLE IF NOT EXISTS bronze_csv.professionnel_sante (
    identifiant TEXT,
    civilite TEXT,
    categorie_professionnelle TEXT,
    nom TEXT,
    prenom TEXT,
    commune TEXT,
    profession TEXT,
    specialite TEXT,
    type_identifiant TEXT
);

CREATE TABLE IF NOT EXISTS bronze_csv.activite_professionnel_sante (
    identifiant TEXT,
    identifiant_organisation TEXT,
    mode_exercice TEXT
);

-- Table des hospitalisations
CREATE SCHEMA IF NOT EXISTS bronze_hospitalisations;

CREATE TABLE IF NOT EXISTS bronze_hospitalisations."Hospitalisations" (
    "Num_Hospitalisation" TEXT,
    "Id_patient" TEXT,
    identifiant_organisation TEXT,
    "Code_diagnostic" TEXT,
    "Suite_diagnostic_consultation" TEXT,
    "Date_Entree" TEXT,
    "Jour_Hospitalisation" TEXT
);
