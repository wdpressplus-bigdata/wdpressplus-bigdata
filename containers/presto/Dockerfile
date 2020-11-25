FROM openjdk:8-jre

RUN apt-get update && apt-get install -y python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PRESTO_VERSION=0.242
ENV PRESTO_HOME=/opt/presto-server-${PRESTO_VERSION}

WORKDIR /opt

# Presto CLI
RUN curl -L -O https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar && \
    chmod +x presto-cli-${PRESTO_VERSION}-executable.jar && \
    ln -s presto-cli-${PRESTO_VERSION}-executable.jar presto-cli

ENV PRESTO_PAGER more

# Presto server
RUN curl -L https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz | tar zxf -

COPY ./etc ${PRESTO_HOME}/etc

RUN groupadd -r presto --gid=1000 && \
    useradd -r -g presto --uid=1000 -d ${PRESTO_HOME} presto && \
    chown presto:presto -R ${PRESTO_HOME} && \
    mkdir -p /var/presto/data && chown presto:presto /var/presto/data

USER presto
EXPOSE 8080

CMD ["/opt/presto-server-0.242/bin/launcher", "run"]
