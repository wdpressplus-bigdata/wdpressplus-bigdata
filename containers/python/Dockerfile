FROM python:3.8
LABEL org.opencontainers.image.source https://github.com/wdpressplus-bigdata/wdpressplus-bigdata

RUN apt-get update && apt-get install -y libpq-dev python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install jupyter pandas psycopg2 pyhive requests scikit-learn sqlalchemy

WORKDIR /opt
