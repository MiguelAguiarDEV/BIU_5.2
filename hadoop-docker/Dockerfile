FROM openjdk:8-jdk

ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_VERSION=3.3.2
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN apt-get update && apt-get install -y ssh rsync wget && \
    wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm /tmp/hadoop.tar.gz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Configura JAVA_HOME en hadoop-env.sh
RUN echo "export JAVA_HOME=/usr/local/openjdk-8" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

# Configuración mínima para pseudo-distribuido
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# RUN echo "127.0.0.1 hadoop-namenode" >> /etc/hosts

EXPOSE 9870 9000 9864

CMD ["/bin/bash"]
