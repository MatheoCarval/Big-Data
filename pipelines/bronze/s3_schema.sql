-- Source S3 : satisfaction (fichiers plats, formats différents selon l'année).
-- Les tables de données (une par fichier, toutes les colonnes en TEXT) sont créées
-- par pipelines/bronze/s3_satisfaction.py à partir de l'en-tête de chaque fichier.
-- `python3 -m pipelines.bronze.s3_satisfaction --ddl` affiche leur DDL.

CREATE SCHEMA IF NOT EXISTS bronze_s3;

-- Catalogue des fichiers ingérés : un fichier (chemin + empreinte) n'est chargé qu'une fois.
CREATE TABLE IF NOT EXISTS bronze_s3.ingestion_fichier (
    _batch_id TEXT NOT NULL,
    _ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source TEXT NOT NULL,
    _fichier_origine TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    taille_octets BIGINT NOT NULL,
    format TEXT NOT NULL,
    encodage TEXT,
    separateur TEXT,
    feuille TEXT,
    table_cible TEXT NOT NULL,
    colonnes_origine TEXT[] NOT NULL,
    nb_lignes_lues INTEGER NOT NULL,
    nb_lignes_ecrites INTEGER,
    UNIQUE (_fichier_origine, sha256)
);
