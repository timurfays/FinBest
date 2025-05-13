# ---------- multi-stage build to cache Spark layer ----------
    FROM alpine:3.19 AS spark-downloader
    ARG SPARK_VERSION=3.2.4
    ARG HADOOP_VERSION=3.2
    
    # Install wget, tar, and ca-certificates for HTTPS support
    RUN apk add --no-cache wget tar ca-certificates \
        && echo "Testing network and DNS resolution" \
        && ping -c 3 archive.apache.org \
        && wget --spider https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
        && wget -q -O /spark.tgz \
           "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
        && mkdir -p /spark \
        && tar --strip-components=1 -xzf /spark.tgz -C /spark
    
    # ---------- Airflow image ----------
    FROM apache/airflow:2.6.3-python3.8
    
    USER root
    
    # System dependencies
    RUN apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
           libpq-dev openjdk-11-jdk wget \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*
    
    RUN apt-get update && apt-get install -y iputils-ping    
    
    # Copy Spark from previous stage
    COPY --from=spark-downloader /spark /opt/spark
    ENV SPARK_HOME=/opt/spark
    ENV PATH=$SPARK_HOME/bin:$PATH
    
    # Download graphframes JAR
    RUN wget -q -O /opt/spark/jars/graphframes-0.8.2-spark3.2-s_2.12.jar \
        https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.2-s_2.12/graphframes-0.8.2-spark3.2-s_2.12.jar \
        && chmod 644 /opt/spark/jars/graphframes-0.8.2-spark3.2-s_2.12.jar
    
    # Copy and install Python dependencies
    COPY requirements.txt /tmp/requirements.txt
    USER airflow
    RUN --mount=type=cache,target=/home/airflow/.cache/pip \
        pip install --no-cache-dir -r /tmp/requirements.txt \
        && PYSPARK_SUBMIT_ARGS="--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell" \
           pip install graphframes
    
    # Default working directory
    WORKDIR /opt/airflow