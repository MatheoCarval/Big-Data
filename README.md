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
├── dags/               DAG Airflow (dag_postgres, dag_etablissements, dag_satisfaction, dag_deces, dag_gold)
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

Extraction de la source S3 (satisfaction) vers PostgreSQL, schéma `bronze_s3` (Python 3 et `psql` suffisent, aucune dépendance) :

```bash
set -a; . ./.env; set +a
createdb -h localhost -U postgres chu_bronze      # une seule fois
python3 -m pipelines.bronze.s3_satisfaction       # relançable sans doublon
```

Le lancement de la plateforme (`docker compose up`) sera documenté ici une fois `infra/` en place.

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
