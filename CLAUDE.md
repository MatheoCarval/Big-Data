# CLAUDE.md

Instructions pour Claude Code et pour tout membre de l'équipe qui travaille sur ce dépôt.
Ce fichier fait référence : en cas de doute, suivre ce qui est écrit ici. S'il est faux ou incomplet, le corriger dans une PR plutôt que de s'en écarter en silence.

---

## 1. Le projet en 30 secondes

Entrepôt de données décisionnel pour le groupe hospitalier **CHU** (bloc « Stockage et Big Data », CESI A4, groupe de 3, 3 semaines).

- **4 sources** → **Lakehouse Bronze / Silver / Gold** sur MinIO + Delta Lake → **Trino** → **API FastAPI** → **front React**.
- Traitements en **PySpark 3.5**, orchestrés par **Airflow**, contrôlés par **Great Expectations**, tracés par **OpenLineage / Marquez**.
- Authentification **Keycloak** avec deux profils métier : **praticien** et **direction**.
- Tout se déploie avec **Docker Compose**. Aucune licence payante.

Document de référence : [docs/cadrage/CHU - Cadrage projet et architecture cible.pdf](docs/cadrage/). Suivi des tâches : [tableau CHU · Pipeline ETL](https://github.com/users/MatheoCarval/projects/2).

On n'utilise **pas** Cloudera, Talend, Hive sur VM ni Power BI. Ils viennent de l'ancien manuel d'installation de l'école et ont été écartés volontairement : ne pas les réintroduire.

---

## 2. Ce qu'on doit livrer

| Livrable | Contenu | Où ça vit dans le dépôt |
|---|---|---|
| **L1** Référentiel et architecture | MCD / MLD, dictionnaire de données, architecture B/S/G, spécifications ETL, plan de gouvernance | `docs/L1/` |
| **L2** Implémentation et performance | Scripts de création / chargement, partitionnement, rapport qualité, ≥ 5 requêtes de benchmark, graphiques avant / après | `pipelines/`, `quality/`, `sql/`, `benchmark/`, `docs/L2/` |
| **L3** Restitution | Application, démo live, storytelling praticiens / direction | `api/`, `front/`, `docs/L3/` |

### Les 8 indicateurs (définitions figées)

| # | Indicateur | Numérateur / dénominateur | Fait |
|---|---|---|---|
| 1 | Taux de consultation par établissement sur une période | consultations de l'établissement / consultations totales de la période | `fait_consultation` |
| 2 | Taux de consultation par diagnostic sur une période | consultations portant le diagnostic / consultations totales de la période | `fait_consultation` |
| 3 | Taux global d'hospitalisation sur une période | patients hospitalisés / patients suivis sur la période | `fait_hospitalisation` |
| 4 | Taux d'hospitalisation par diagnostic | hospitalisations portant le diagnostic / hospitalisations totales | `fait_hospitalisation` |
| 5 | Taux d'hospitalisation par sexe et âge | répartition des hospitalisations par sexe et tranche d'âge à l'admission | `fait_hospitalisation` |
| 6 | Taux de consultation par professionnel | consultations du professionnel / consultations totales | `fait_consultation` |
| 7 | Nombre de décès par région en 2019 | comptage par région de décès | `fait_deces` |
| 8 | Satisfaction globale par région en 2020 | score moyen des établissements de la région | `fait_satisfaction` |

Ne pas changer une définition sans mettre à jour le dictionnaire de données **et** prévenir l'équipe dans la PR.

---

## 3. Sources

| Id | Source | Accès | Fichiers / tables | Alimente |
|---|---|---|---|---|
| **S1** | PostgreSQL 14 opérationnel | JDBC | 13 tables : `Patient`, `Consultation`, `Diagnostic`, `Professionnel_de_sante`, `Specialites`, `Prescription`, `Medicaments`, `Laboratoire`, `Mutuelle`, `Adher`, `Salle`, `date`, `AAAA` | faits consultation / hospitalisation, dims patient, diagnostic, professionnel |
| **S2** | Référentiel établissements | fichiers CSV | `etablissement_sante.csv`, `professionnel_sante.csv`, `activite_professionnel_sante.csv` | `dim_etablissement`, `dim_localisation` |
| **S3** | Satisfaction | FTP | `Satisfaction/<année>/*.csv` : **le format change selon l'année** | `fait_satisfaction` |
| **S4** | Décès INSEE | FTP, open data | `deces.csv` : volumineux (dizaines de millions de lignes) | `fait_deces` |
| — | Hospitalisations | CSV | `Hospitalisation/Hospitalisations.csv` | `fait_hospitalisation` (origine à confirmer, voir issue #14) |

- Les données brutes sont dans `data/` (non versionné). Pack d'origine : **DATA 2024**.
- En local, la base S1 est restaurée dans PostgreSQL 14 : base `postgres`, schéma `public`. Les noms de tables contiennent des **majuscules** : toujours les citer entre guillemets en SQL (`"Patient"`).
- On **ne modifie jamais** une source. On l'intègre telle quelle.

---

## 4. Architecture et flux

```
S1 PostgreSQL ─┐  Spark JDBC
S2 CSV        ─┤  tâches Python          ┌──────────── Lakehouse : MinIO + Delta ────────────┐
S3 FTP        ─┼────────────────────────►│ BRONZE ──► SILVER ──► [Great Expectations] ──► GOLD │
S4 FTP INSEE  ─┘                         └─────────────────────────────────────────────────────┘
                                                        Airflow orchestre, OpenLineage trace   │
                                                                                               ▼
                                         Keycloak ──► React ◄── FastAPI ◄── Trino (lit Gold uniquement)
```

### Règles par couche (non négociables)

| Couche | Ce qu'on y met | Règles |
|---|---|---|
| **Bronze** | Copie brute, historisée | **Append-only**, aucune transformation, colonnes techniques ajoutées (voir §6). Seule couche où des données identifiantes peuvent exister. |
| **Silver** | Une table propre par entité métier | Schéma imposé, typé, dédoublonné, codes normalisés, **patients pseudonymisés**, lignes invalides envoyées en table de rejets. **Aucune donnée directement identifiante.** |
| **Gold** | Schéma en étoile (constellation) + agrégats | Clés de substitution, partitionné, optimisé. **Seule couche lue par l'application**, via Trino. |

Un contrôle Great Expectations **bloquant** en échec arrête le pipeline : Gold n'est pas mise à jour et l'application continue de servir la dernière version valide.

---

## 5. Modèle décisionnel (Gold)

Approche Kimball, schéma en constellation : 4 faits partagent des dimensions conformes.

### Faits

| Table | Grain | Mesures | Partition |
|---|---|---|---|
| `fait_consultation` | une consultation | `nb_consultations`, `age_patient_acte` | `annee` ; buckets sur `id_patient_pseudo`, `id_diagnostic` |
| `fait_hospitalisation` | un séjour | `nb_sejours`, `duree_sejour_jours`, `age_admission` | `annee` |
| `fait_deces` | un décès INSEE | `nb_deces`, `age_deces` | `annee`, `region` |
| `fait_satisfaction` | un établissement × une année d'enquête | `score_satisfaction`, `nb_repondants` | `annee`, `region` |

### Matrice de bus

| Dimension | Consultation | Hospitalisation | Décès | Satisfaction |
|---|:-:|:-:|:-:|:-:|
| `dim_temps` | ✔ | ✔ | ✔ | ✔ |
| `dim_etablissement` | ✔ | ✔ | — | ✔ |
| `dim_localisation` | via étab. | via étab. | ✔ | via étab. |
| `dim_patient` | ✔ | ✔ | — | — |
| `dim_diagnostic` | ✔ | ✔ | — | — |
| `dim_professionnel` | ✔ | — | — | — |

### Dimensions

| Table | Clé | Attributs |
|---|---|---|
| `dim_temps` | `id_date` (int AAAAMMJJ) | date, jour, mois, trimestre, annee, jour_semaine |
| `dim_localisation` | `id_commune` (code INSEE, 5 car.) | commune, departement, region |
| `dim_etablissement` | `id_etablissement` (substitution) | code_finess, raison_sociale, categorie, capacite, id_commune |
| `dim_patient` | `id_patient_pseudo` | sexe, annee_naissance, id_commune |
| `dim_diagnostic` | `id_diagnostic` (substitution) | code, libelle, categorie |
| `dim_professionnel` | `id_professionnel` (substitution) | specialite, id_etablissement |

- Toutes les clés sont des **clés de substitution** générées par le pipeline ; la **clé métier d'origine est conservée** en attribut (`code_*` ou `*_source`).
- Chaque dimension contient un membre **« Non renseigné »** (clé `-1`) vers lequel pointent les faits sans correspondance.
- Delta n'a pas de clés étrangères contraignantes : l'intégrité référentielle est garantie par le pipeline et **vérifiée par Great Expectations**.

---

## 6. Conventions

### Nommage

| Élément | Convention | Exemple |
|---|---|---|
| Tables Bronze | `<source>_<table_source_en_snake_case>` | `s1_consultation`, `s2_etablissement_sante`, `s4_deces` |
| Tables Silver | `<entite>` | `patient`, `consultation`, `etablissement` |
| Tables de rejets | `rejets_<source>` | `rejets_s1` |
| Tables Gold | `dim_*`, `fait_*`, `agg_*` | `fait_consultation`, `agg_satisfaction_region` |
| Colonnes | `snake_case`, français, sans accents | `date_admission`, `code_finess` |
| Colonnes techniques | préfixe `_` | `_ingestion_ts`, `_source`, `_batch_id`, `_fichier_origine` |
| DAG Airflow | `dag_<source>` / `dag_gold` | `dag_postgres` |
| Branches Git | `feat/<n°-issue>-<sujet>`, `fix/…`, `docs/…` | `feat/12-extract-postgres` |

### Stockage

```
s3a://bronze/<source>/<table>/        tables Delta brutes
s3a://silver/<entite>/                tables Delta propres
s3a://silver/_rejets/<source>/        rejets
s3a://gold/<dim_* | fait_* | agg_*>/  modèle en étoile
```

Bases du Hive Metastore : `bronze`, `silver`, `gold`. Trino n'expose que `gold`.

### Règles de transformation

- **Typage** : schéma explicite à la lecture (`StructType`), jamais `inferSchema` en production.
- **FINESS** : 9 caractères. **INSEE** : 5 caractères avec zéros initiaux (`lpad`).
- **Libellés de jointure** : majuscules, sans accents.
- **Nulls** : attribut obligatoire nul → ligne en rejet ; attribut descriptif nul → `"Non renseigné"`.
- **Doublons** : détection sur la clé métier, on garde le plus récent, on **compte** les écartés.
- **Dérivées** : âge à la date de l'acte / de l'admission, tranche d'âge (mêmes tranches partout), région déduite du code commune.
- **Chargement** : `MERGE` Delta sur la clé métier, jamais `overwrite` complet en Silver / Gold.

### Rejets et journalisation

Chaque job écrit, pour chaque exécution : **lignes lues / écrites / rejetées**. Une ligne rejetée va dans `rejets_<source>` avec `motif`, `_ingestion_ts`, `job`, et la ligne d'origine.

---

## 7. RGPD et sécurité (à respecter dans chaque PR)

Les données de santé sont des données sensibles (article 9 du RGPD).

1. **Pseudonymisation** : l'identifiant patient devient `sha2(concat(id, PSEUDO_SALT), 256)` **dès Silver**. Le sel vient de l'environnement, jamais du code.
2. **Minimisation** : noms, prénoms, adresses, téléphones **ne dépassent jamais Bronze**.
3. **Généralisation** : date de naissance → année (Silver) → tranche d'âge (Gold / application).
4. **Seuil de diffusion** : l'API **masque tout résultat portant sur moins de 10 patients**.
5. **Requêtes paramétrées uniquement** dans l'API : aucun texte saisi par l'utilisateur n'est concaténé dans du SQL.
6. **Secrets** : uniquement dans `.env` (non versionné) ou les secrets Airflow. `.env.example` liste les variables sans valeur.
7. **Données** : rien de `data/`, aucun extrait réel, aucun CSV de test contenant de vraies lignes n'est commité. Les tests utilisent des données synthétiques.

Avant chaque commit, vérifier que le diff ne contient ni mot de passe, ni sel, ni donnée réelle.

---

## 8. Workflow Git (les branches sont protégées)

- `main` et `dev` sont **protégées**, admins compris : **pas de push direct**, PR obligatoire, **1 approbation d'une autre personne que celle qui a poussé en dernier**, conversations résolues, pas de force-push.
- `main` = livré et stable, mis à jour uniquement par PR depuis `dev`.
- `dev` = intégration, branche par défaut.

Cycle :

```bash
git switch dev && git pull
git switch -c feat/12-extract-postgres
# ... travail, commits ...
git push -u origin feat/12-extract-postgres
gh pr create --base dev --fill    # mettre "Closes #12" dans la description
```

- Commits au format **Conventional Commits** : `feat(bronze): …`, `fix(silver): …`, `docs: …`, `chore(infra): …`, `test: …`.
- Une PR = une issue = un sujet. Petites PR, faciles à relire.
- Fusion en **squash**, la branche est supprimée automatiquement.
- Mettre à jour le statut de l'issue dans le tableau (Todo → In Progress → Done).

---

## 9. Consignes pour Claude Code

Quand tu travailles sur ce dépôt :

1. **Lis l'issue** liée à la tâche (`gh issue view <n>`) : ses critères d'acceptation définissent « terminé ».
2. **Ne pousse jamais sur `main` ou `dev`** et ne tente pas de contourner la protection : crée une branche `feat/…` et ouvre une PR vers `dev`. Ne fusionne jamais une PR toi-même.
3. **Respecte la couche** : un script `pipelines/bronze/` ne transforme rien ; un script `pipelines/silver/` ne lit que Bronze ; un script `pipelines/gold/` ne lit que Silver ; l'API ne lit que Gold via Trino.
4. **Idempotence** : tout job doit pouvoir être relancé sans créer de doublon (MERGE sur clé métier, `_batch_id`).
5. **Jamais de donnée réelle** dans le code, les tests, les notebooks commités ou les messages de commit. N'affiche pas de lignes patient réelles dans tes réponses ; préfère des comptages et des schémas.
6. **Pas de secret en dur** : lis `os.environ[...]` et ajoute toute nouvelle variable à `.env.example`.
7. **Réutilise** `pipelines/common/` (session Spark, écriture Bronze, rejets, journalisation) au lieu de réécrire la logique.
8. **Teste** : ajoute un test dans `tests/` pour toute règle de transformation, avec des données synthétiques.
9. **Documente** : toute nouvelle table ou colonne Gold va dans le dictionnaire de données (`docs/L1/`).
10. **Reste dans la stack** : open source uniquement, et rien qui n'existe pas dans Docker Compose sans en discuter dans la PR.
11. Rédige en **français** (code : identifiants en français sans accents, commentaires rares et utiles).
12. Si une consigne de ce fichier est fausse ou bloque, **dis-le et propose une correction de `CLAUDE.md`** plutôt que de l'ignorer.

---

## 10. Structure du dépôt

```
.
├── CLAUDE.md               ce fichier
├── README.md               présentation et démarrage
├── .env.example            variables d'environnement attendues
├── infra/                  docker-compose.yml, config MinIO, Spark, Hive Metastore, Trino, Airflow, Keycloak, Marquez
├── dags/                   DAG Airflow : dag_postgres, dag_etablissements, dag_hospitalisations, dag_satisfaction, dag_deces, dag_gold
├── pipelines/
│   ├── common/             session Spark, écriture Bronze, rejets, journalisation, pseudonymisation
│   ├── bronze/             EXTRACT   : sources → Bronze
│   ├── silver/             TRANSFORM : Bronze → Silver
│   └── gold/               LOAD      : Silver → Gold, optimisation
├── quality/                suites Great Expectations
├── sql/                    requêtes des 8 indicateurs (Trino)
├── benchmark/              protocole, scripts et résultats avant / après
├── api/                    FastAPI : auth Keycloak, requêtes paramétrées, seuil < 10
├── front/                  React : ECharts, Leaflet, vues par profil
├── tests/                  tests unitaires (données synthétiques)
├── docs/
│   ├── cadrage/            document de cadrage
│   ├── L1/ L2/ L3/         livrables
└── data/                   DATA 2024 en local, jamais versionné
```

---

## 11. Optimisation et benchmark (livrable 2)

Techniques attendues : Parquet / Delta (colonnaire), partitionnement (`annee`, `region`), bucketing (clé patient, clé diagnostic), Z-order (établissement, diagnostic), `OPTIMIZE`, `ANALYZE TABLE`, broadcast des petites dimensions, `VACUUM` avec rétention définie.

Protocole : mêmes données en **CSV non partitionné** vs **Delta optimisé** ; **≥ 6 requêtes** (une par indicateur obligatoire + décès par région) ; **5 exécutions, la première écartée, médiane retenue** ; comparaison **Spark SQL vs Trino** ; volume lu relevé dans les plans ; graphiques avant / après dans `benchmark/`.

---

## 12. Application (livrable 3)

| Vue | Profil | Contenu |
|---|---|---|
| Activité de consultation | praticien + direction | indicateurs 1, 2, 6, filtres par période |
| Hospitalisations | praticien | indicateurs 3, 4, 5, pyramide sexe / âge, évolution mensuelle |
| Pilotage des établissements | direction | comparaison, capacités, indicateur 8 sur carte |
| Mortalité | direction | indicateur 7 sur carte Leaflet |
| Performance technique | équipe projet | temps avant / après, requête live pendant la soutenance |

Rôles Keycloak : `praticien`, `direction`, `data_engineer`, `admin_securite`. L'API vérifie le rôle à chaque appel et renvoie 403 sinon.

---

## 13. Commandes utiles

```bash
# Tâches du projet
gh issue list --label extract            # ou transform, load, socle, restitution
gh issue view 12

# Source S1 en local (PostgreSQL 14 installé sur le poste)
psql -h localhost -U postgres -d postgres -c '\dt'

# Plateforme (le .env est à la racine, le compose dans infra/)
docker compose --env-file .env -f infra/docker-compose.yml up -d --build

# FTP de simulation S3 / S4 : poste -> localhost:2121, conteneurs du réseau chu -> ftp:21
set -a; . ./.env; set +a
python -m pipelines.common.ftp initialiser "data"                 # dépôt initial DATA 2024 (compte FTP_DEPOT_USER)
python -m pipelines.common.ftp deposer nouveau.csv satisfaction/2021
python -m pipelines.common.ftp telecharger satisfaction data/ftp/satisfaction   # compte lecture seule FTP_USER

# Tests du client FTP (pyftpdlib requis : on les lance dans l'image du serveur)
docker run --rm -v "$PWD:/app" -w /app chu-ftp:local python -m unittest tests.test_ftp

# Ingestion des CSV S2 + Hospitalisations vers Bronze (LAKE_ROOT local = pas de S3A)
export DATA_DIR=data LAKE_ROOT=data/lac
python -m pipelines.bronze.csv_bronze [--table s2_etablissement_sante]

# Tests Spark (Windows : pas de winutils, on passe par Docker ; Git Bash : MSYS_NO_PATHCONV=1 et chemin C:/…)
docker run --rm -v "$PWD:/app:ro" -w /app python:3.11-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq openjdk-17-jre-headless \
   && pip install -q "pyspark==3.5.*" "delta-spark==3.2.*" \
   && PYTHONDONTWRITEBYTECODE=1 python -m unittest tests.test_csv_bronze tests.test_normalize'
```

Toute commande ajoutée au projet (lancer un DAG, un benchmark, les tests) doit être documentée ici et dans le README.
