CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.s2_etablissement_sante (
    adresse STRING,
    cedex STRING,
    code_commune STRING,
    code_postal STRING,
    commune STRING,
    complement_destinataire STRING,
    complement_point_geographique STRING,
    email STRING,
    enseigne_commerciale_site STRING,
    finess_etablissement_juridique STRING,
    finess_site STRING,
    identifiant_organisation STRING,
    indice_repetition_voie STRING,
    mention_distribution STRING,
    numero_voie STRING,
    pays STRING,
    raison_sociale_site STRING,
    siren_site STRING,
    siret_site STRING,
    telecopie STRING,
    telephone STRING,
    telephone_2 STRING,
    type_voie STRING,
    voie STRING,
    _ingestion_ts TIMESTAMP,
    _source STRING,
    _batch_id STRING,
    _fichier_origine STRING
)
USING DELTA
LOCATION 's3a://bronze/s2/etablissement_sante'
TBLPROPERTIES (delta.appendOnly = true);

CREATE TABLE IF NOT EXISTS bronze.s2_professionnel_sante (
    identifiant STRING,
    civilite STRING,
    categorie_professionnelle STRING,
    nom STRING,
    prenom STRING,
    commune STRING,
    profession STRING,
    specialite STRING,
    type_identifiant STRING,
    _ingestion_ts TIMESTAMP,
    _source STRING,
    _batch_id STRING,
    _fichier_origine STRING
)
USING DELTA
LOCATION 's3a://bronze/s2/professionnel_sante'
TBLPROPERTIES (delta.appendOnly = true);

CREATE TABLE IF NOT EXISTS bronze.s2_activite_professionnel_sante (
    identifiant STRING,
    identifiant_organisation STRING,
    mode_exercice STRING,
    _ingestion_ts TIMESTAMP,
    _source STRING,
    _batch_id STRING,
    _fichier_origine STRING
)
USING DELTA
LOCATION 's3a://bronze/s2/activite_professionnel_sante'
TBLPROPERTIES (delta.appendOnly = true);

CREATE TABLE IF NOT EXISTS bronze.hosp_hospitalisations (
    num_hospitalisation STRING,
    id_patient STRING,
    identifiant_organisation STRING,
    code_diagnostic STRING,
    suite_diagnostic_consultation STRING,
    date_entree STRING,
    jour_hospitalisation STRING,
    _ingestion_ts TIMESTAMP,
    _source STRING,
    _batch_id STRING,
    _fichier_origine STRING
)
USING DELTA
LOCATION 's3a://bronze/hosp/hospitalisations'
TBLPROPERTIES (delta.appendOnly = true);
