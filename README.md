# CHU · Cloud Healthcare Unit

Entrepôt de données décisionnel pour le groupe hospitalier CHU, réalisé pendant le bloc « Stockage et Big Data » (CESI A4).

La plateforme collecte quatre sources de données de santé, les nettoie et les pseudonymise, les organise en schéma en étoile, puis les restitue dans une application web adaptée aux praticiens et à la direction.

## Architecture

```
Sources            Lakehouse (MinIO + Delta Lake)            Restitution
───────            ──────────────────────────────            ───────────
PostgreSQL  ─┐
CSV FINESS  ─┼─► Bronze ──► Silver ──► Gold ──► Trino ──► FastAPI ──► React
FTP satisf. ─┤   brut       propre,    étoile,                │
FTP décès   ─┘              pseudo.    optimisé           Keycloak (rôles)

             Spark 3.5 · Great Expectations · Airflow · OpenLineage
```

| Couche | Contenu | Règle |
|---|---|---|
| **Bronze** | Copie brute des sources, historisée | Ajout seulement, aucune transformation |
| **Silver** | Données typées, dédoublonnées, pseudonymisées | Aucune donnée directement identifiante |
| **Gold** | Dimensions et faits du schéma en étoile | Seule couche exposée à l'application |

Le détail des choix est dans [docs/cadrage](docs/cadrage/).

## Stack

| Brique | Outil |
|---|---|
| Déploiement | Docker Compose |
| Stockage | MinIO (S3) + Delta Lake |
| Traitement | Apache Spark 3.5 (PySpark) |
| Catalogue / requêtes | Hive Metastore, Trino |
| Orchestration | Apache Airflow |
| Qualité / lignage | Great Expectations, OpenLineage + Marquez |
| Application | FastAPI, React, ECharts, Leaflet |
| Authentification | Keycloak |

## Structure du dépôt

```
.
├── infra/              docker-compose et configuration des services
├── dags/               DAG Airflow (dag_postgres, dag_etablissements, dag_hospitalisations, dag_satisfaction, dag_deces, dag_gold)
├── pipelines/
│   ├── common/         session Spark, écriture Bronze, tables de rejets, journalisation
│   ├── bronze/         Extract : sources → Bronze
│   ├── silver/         Transform : Bronze → Silver
│   └── gold/           Load : Silver → Gold (dimensions, faits, optimisation)
├── quality/            suites Great Expectations
├── sql/                requêtes des 8 indicateurs
├── benchmark/          protocole et résultats avant / après optimisation
├── api/                API FastAPI
├── front/              application React
├── tests/              tests unitaires des pipelines
├── docs/               cadrage, livrables L1 / L2 / L3, dictionnaire de données
└── data/               données locales DATA 2024 (non versionnées)
```

## Démarrage

Prérequis : Docker Desktop, Git, Python 3.11+, Node 20+.

```bash
git clone https://github.com/MatheoCarval/Big-Data.git
cd Big-Data
cp .env.example .env        # puis renseigner les mots de passe et le sel
```

Copier le contenu du pack **DATA 2024** dans `data/` (le dossier n'est pas versionné : données de santé).

### Plateforme

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --build
```

Services disponibles pour l'instant :

| Service | Accès depuis le poste | Accès depuis un conteneur (réseau `chu`) |
|---|---|---|
| MinIO, API S3 (buckets `bronze`, `silver`, `gold`) | `http://localhost:9000` | `http://minio:9000` |
| MinIO, console | `http://localhost:9001` | — |
| Airflow | `http://localhost:8080` | — |
| FTP de simulation (sources S3 et S4) | `ftp://localhost:2121` | `ftp://ftp:21` |

### FTP de simulation (S3 satisfaction, S4 décès)

Le serveur reproduit le dépôt FTP des sources : `/satisfaction/<annee>/…` et `/deces/…`. Deux comptes, définis dans `.env` :

- `FTP_USER` : lecture seule, utilisé par les pipelines et Airflow ;
- `FTP_DEPOT_USER` : écriture, simule le producteur qui dépose les fichiers.

```bash
set -a; . ./.env; set +a

# une seule fois : déposer les fichiers du pack DATA 2024 (≈ 2 Go, relançable sans doublon)
python -m pipelines.common.ftp initialiser "data"

# simuler l'arrivée d'un nouveau fichier
python -m pipelines.common.ftp deposer nouveau.csv satisfaction/2021

# côté pipelines : lister et récupérer
python -m pipelines.common.ftp lister satisfaction
python -m pipelines.common.ftp telecharger satisfaction data/ftp/satisfaction
```

Les fichiers sont transférés sous un nom temporaire `.part` puis renommés : un capteur ne voit jamais un fichier incomplet. Les données déposées sont conservées dans le volume Docker `chu_ftp_depot`.

### Ingestion des CSV vers Bronze (S2 et Hospitalisations)

`pipelines/bronze/csv_bronze.py` copie les 4 CSV de `data/` (`Etablissement de SANTE/` et `Hospitalisation/`) dans les tables Delta Bronze décrites par `pipelines/bronze/CSV_schema.sql`, sans aucune transformation. Il nécessite `pyspark==3.5.*`, `delta-spark==3.2.*` et Java 17 (sous Windows, Spark exige en plus `winutils` : passer par Docker ou WSL).

```bash
# DATA_DIR : dossier des CSV (défaut « data »). LAKE_ROOT : racine du lac (défaut « s3a:// », soit MinIO ;
# voir MINIO_ENDPOINT dans .env). Un dossier local (ici data/lac, ignoré par git) désactive S3A.
export DATA_DIR=data LAKE_ROOT=data/lac

python -m pipelines.bronze.csv_bronze                                # les 4 fichiers
python -m pipelines.bronze.csv_bronze --table s2_etablissement_sante # une seule table
```

Tests Spark (données synthétiques). Sous Windows, Spark ne démarre pas sans `winutils` : on les lance dans un conteneur Java 17 (~1 min après le premier téléchargement de l'image) :

```bash
# Git Bash sous Windows : préfixer par MSYS_NO_PATHCONV=1 et écrire le chemin « C:/repos/Big-Data » à la place de $PWD
docker run --rm -v "$PWD:/app:ro" -w /app python:3.11-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq openjdk-17-jre-headless \
   && pip install -q "pyspark==3.5.*" "delta-spark==3.2.*" \
   && PYTHONDONTWRITEBYTECODE=1 python -m unittest tests.test_csv_bronze'
```

Relancer le job sur un fichier de même contenu ne recharge rien (le `_batch_id` est l'empreinte du fichier). Chaque exécution ajoute une ligne « lues / écrites / rejetées » à `<LAKE_ROOT>/bronze/_journal` ; les lignes illisibles vont dans `<LAKE_ROOT>/silver/_rejets/<source>`.

#### DAG `dag_etablissements`

`dags/dag_etablissements.py` relance chaque mois l'ingestion des 3 fichiers S2 : une tâche par table, exécutées l'une après l'autre (elles partagent le journal et les rejets). Un référentiel inchangé est ignoré. Le DAG tourne dans l'Airflow du compose (`http://localhost:8080`, `LAKE_ROOT=s3a://` vers MinIO) ; ses tests utilisent un Airflow simulé et ne nécessitent ni Airflow ni Spark.

`dags/dag_hospitalisations.py` fait de même, chaque mois, pour `Hospitalisations.csv` (une seule tâche).

```bash
python -m unittest tests.test_dag_etablissements tests.test_dag_hospitalisations
```

## Travailler sur le projet

Les tâches sont suivies dans le tableau [CHU · Pipeline ETL](https://github.com/users/MatheoCarval/projects/2), découpé en phases Socle → Extract → Transform → Load → Restitution.

**Branches**

| Branche | Rôle |
|---|---|
| `main` | Version stable, livrée. Mise à jour uniquement depuis `dev`. |
| `dev` | Intégration. Toutes les fonctionnalités y sont fusionnées. |
| `feat/<n°-issue>-<sujet>` | Une branche par tâche, créée depuis `dev`. |

**Cycle d'une tâche**

1. Prendre une issue du tableau et se l'assigner.
2. `git switch dev && git pull`, puis `git switch -c feat/12-extract-postgres`.
3. Commits courts, au présent : `feat(bronze): extraction JDBC des 13 tables`.
4. Pousser et ouvrir une Pull Request vers `dev` avec `Closes #12` dans la description.
5. **Une autre personne du groupe relit et approuve** : on ne fusionne jamais sa propre PR.
6. Fusion en *squash*, suppression de la branche.

**Règles**

- Aucune donnée, aucun mot de passe, aucun sel dans le dépôt : tout passe par `.env`.
- Les jobs sont idempotents : relancer un job ne crée pas de doublon.
- Les noms, prénoms, adresses et téléphones ne dépassent jamais la couche Bronze.

## Équipe

Projet réalisé en groupe de trois, CESI École d'ingénieurs, A4.
