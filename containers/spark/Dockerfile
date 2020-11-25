FROM python:3.8

RUN apt-get update && apt-get install -y default-jre-headless libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# http://blog.stuart.axelbrooke.com/python-3-on-spark-return-of-the-pythonhashseed
ENV PYTHONHASHSEED 0
ENV PYTHONIOENCODING UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK 1

WORKDIR /opt

ENV SPARK_VERSION 3.0.1
ENV SPARK_HOME /opt/spark-${SPARK_VERSION}-bin-hadoop3.2
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz | tar zxf -

ENV SPARK_LOCAL_IP 0.0.0.0
ENV PATH "$SPARK_HOME/bin:$PATH"

# Install Spark packages
RUN spark-submit run-example --packages org.apache.hadoop:hadoop-aws:3.2.0 SparkPi 1

# Install Python packages
RUN pip install pandas psycopg2 sqlalchemy

COPY ./spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
