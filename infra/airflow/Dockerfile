FROM apache/airflow:2.10.5-python3.11

# Les versions Delta / Hadoop doivent rester alignées sur pyspark 3.5 et sur HADOOP_AWS de
# pipelines/common/session_spark.py
ARG DELTA_VERSION=3.2.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java

USER airflow
RUN pip install --no-cache-dir "pyspark==3.5.*" "delta-spark==${DELTA_VERSION}"

# JAR embarqués dans l'image : Spark ne télécharge rien au démarrage (DELTA_JARS_INCLUS=1)
RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && for jar in \
        io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
        io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
        org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar; do \
        curl -fsSL -o "${JARS}/$(basename "${jar}")" "https://repo1.maven.org/maven2/${jar}"; \
    done
