"""DAG mensuel : référentiel des établissements et des professionnels (S2) vers Bronze.

Une tâche par fichier CSV ; chacune lance pipelines.bronze.csv_bronze sur une table. DATA_DIR et
LAKE_ROOT sont lus dans l'environnement du worker. Un fichier inchangé est ignoré (même _batch_id),
donc un déclenchement mensuel sans nouvelle version du référentiel ne recharge rien.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Doit rester identique aux tables « s2 » de csv_bronze.CATALOGUE
TABLES_S2 = (
    "s2_etablissement_sante",
    "s2_professionnel_sante",
    "s2_activite_professionnel_sante",
)

# ingerer table S2 vers Bronze : on ne charge Spark qu'une fois par table, pas à l'import du DAG
def ingerer_table(table):
    # import ici : le scheduler analyse ce fichier en boucle, il n'a pas à charger Spark
    from pipelines.bronze import csv_bronze

    code = csv_bronze.main(["--table", table])
    if code != 0:
        raise RuntimeError(f"Ingestion Bronze en échec pour {table} (voir <LAKE_ROOT>/bronze/_journal)")


# with DAG sert à créer un contexte dans lequel les tâches sont automatiquement ajoutées au DAG
with DAG(
    dag_id="dag_etablissements",
    description="S2 : établissements et professionnels de santé vers Bronze",
    schedule="@monthly", # mensuel, le 1er du mois à 00:00 UTC
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    # les tâches partagent bronze/_journal et silver/_rejets/s2 : une seule à la fois
    max_active_tasks=1, # Nb max de tâches actives simultanément dans le DAG
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["bronze", "s2"],
) as dag:
    # Une tâche par table S2, chacune lance pipelines.bronze.csv_bronze sur la table
    for table in TABLES_S2:
        PythonOperator(
            task_id=f"bronze_{table}",
            python_callable=ingerer_table,
            op_kwargs={"table": table},
        )
