FROM openjdk:17-jdk-slim

ARG SPARK_VERSION=3.3.2
ARG HADOOP_VERSION=3

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get update && apt-get install -y wget procps python3 python3-pip && \
    wget -qO /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz && \
    wget -qO /opt/spark/jars/mariadb-java-client.jar https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.3.3/mariadb-java-client-3.3.3.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark
EXPOSE 4040 8080 7077 6066

CMD ["spark-shell"]
