"""DAG des hospitalisations (CSV) vers Bronze.

Une seule tâche : pipelines.bronze.csv_bronze sur la table hosp_hospitalisations. DATA_DIR et LAKE_ROOT
sont lus dans l'environnement du worker. Un fichier inchangé est ignoré (même _batch_id).
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Doit rester identique aux tables « hosp » de csv_bronze.CATALOGUE
TABLES_HOSP = ("hosp_hospitalisations",)


def ingerer_table(table):
    # import ici : le scheduler analyse ce fichier en boucle, il n'a pas à charger Spark
    from pipelines.bronze import csv_bronze

    code = csv_bronze.main(["--table", table])
    if code != 0:
        raise RuntimeError(f"Ingestion Bronze en échec pour {table} (voir <LAKE_ROOT>/bronze/_journal)")


with DAG(
    dag_id="dag_hospitalisations",
    description="Hospitalisations (CSV) vers Bronze",
    schedule="@monthly",  # mensuel, le 1er du mois à 00:00 UTC
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # deux exécutions simultanées du même fichier pourraient le charger deux fois
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["bronze", "hosp"],
) as dag:
    for table in TABLES_HOSP:
        PythonOperator(
            task_id=f"bronze_{table}",
            python_callable=ingerer_table,
            op_kwargs={"table": table},
        )