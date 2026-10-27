"""Session Spark commune aux jobs : Delta Lake, et S3A vers MinIO quand le lac est sur s3a://.

    from pipelines.common.session_spark import creer_session
    spark = creer_session("csv_bronze")

Variables d'environnement :
    LAKE_ROOT            racine du lac, « s3a:// » par défaut ; un dossier local désactive S3A
    MINIO_ENDPOINT       URL de MinIO, lue seulement si le lac est sur s3a://
                         (http://localhost:9000 depuis le poste, http://minio:9000 depuis le réseau chu)
    MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
                         identifiants MinIO, lus seulement si le lac est sur s3a://
    SPARK_DRIVER_MEMORY  mémoire du driver, « 2g » par défaut
    DELTA_JARS_INCLUS    « 1 » si les JAR Delta et hadoop-aws sont déjà dans l'image
                         (infra/airflow/Dockerfile) ; sinon ils sont téléchargés au démarrage
"""

import os

# Doit rester identique à HADOOP_AWS_VERSION de infra/airflow/Dockerfile
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"


def lac_sur_s3():
    return (os.environ.get("LAKE_ROOT") or "s3a://").startswith("s3a://")


def configurer_s3a(builder):
    """Ajoute à `builder` la configuration S3A vers MinIO (adressage par chemin, pas de TLS en local)."""
    endpoint = os.environ["MINIO_ENDPOINT"]
    return (
        builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
    )


def creer_session(nom="chu"):
    """Session Spark avec les extensions Delta Lake, et S3A si le lac est sur s3a://."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(nom)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.enabled", "false")
    )
    sur_s3 = lac_sur_s3()
    if sur_s3:
        builder = configurer_s3a(builder)
    if os.environ.get("DELTA_JARS_INCLUS") != "1":
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder, extra_packages=[HADOOP_AWS] if sur_s3 else None)
    return builder.getOrCreate()
