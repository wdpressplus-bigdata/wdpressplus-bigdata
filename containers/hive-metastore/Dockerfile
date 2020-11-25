FROM openjdk:8-jre

WORKDIR /opt

ENV HADOOP_VERSION=3.2.0
ENV METASTORE_VERSION=3.0.0

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

RUN curl -L https://downloads.apache.org/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zxf - && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf - && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.2.14.jar -o ${HIVE_HOME}/lib/postgresql-42.2.14.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -o ${HIVE_HOME}/lib/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -o ${HIVE_HOME}/lib/aws-java-sdk-bundle-1.11.271.jar

COPY ./conf/metastore-site.xml ${HIVE_HOME}/conf/metastore-site.xml
COPY ./initSchema /initSchema

RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chmod +x /initSchema

USER hive
EXPOSE 9083

CMD ["/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore"]
