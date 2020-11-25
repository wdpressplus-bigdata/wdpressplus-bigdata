This repository contains several containers described in the book.

## Containers and ports

Some containers expose TCP ports (defined in docker-compose.yml):

| Container | Port | Description |
| --- | --- | --- |
| datamart | | PostgreSQL |
| metabase | 13000 | Visualization (Metabase) |
| metastore | | Hive metastore |
| metastore-db | | PostgreSQL for Hive metastore |
| minio | 9000 | Object storage (MinIO) |
| presto-server | | Presto server |

The following containers are also created by `prefect server start`:

| Container | Port | Description |
| --- | --- | --- |
| postgres | 5432 | PostgreSQL |
| hasura | 3000 | GraphQL server |
| graphql | 4201 | GraphQL server |
| apollo | 4200 | GraphQL server |
| towel | | Prefect server |
| ui | 8080 | Prefect Web UI |

## Launch containers

```
$ cd wdpressplus-bigdata

$ docker-compose up -d minio
Creating network "wdpressplus-bigdata_default" with the default driver
Creating wdpressplus-bigdata_minio_1 ... done

$ docker-compose up -d metastore-db
Creating wdpressplus-bigdata_metastore-db_1 ... done

$ docker-compose run --rm metastore /initSchema
...
1/1206       --
2/1206       -- PostgreSQL database dump
3/1206       --
...
1203/1206    -- -----------------------------------------------------------------
1204/1206    -- Record schema version. Should be the last step in the init script
1205/1206    -- -----------------------------------------------------------------
1206/1206    INSERT INTO "VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '3.0.0', 'Hive release version 3.0.0');
1 row affected (0.01 seconds)
Closing: org.postgresql.jdbc.PgConnection
sqlline version 1.3.0
Initialization script completed
schemaTool completed
...

$ docker-compose up -d metastore
Creating wdpressplus-bigdata_metastore_1 ... done

$ docker-compose up -d presto-server
Recreating wdpressplus-bigdata_presto-server_1 ... done

$ docker-compose up -d datamart
Creating wdpressplus-bigdata_datamart_1 ... done

$ docker-compose up -d metabase
Creating wdpressplus-bigdata_metabase_1 ... done

$ docker-compose ps
               Name                              Command               State            Ports
------------------------------------------------------------------------------------------------------
wdpressplus-bigdata_datamart_1        docker-entrypoint.sh postgres    Up      5432/tcp
wdpressplus-bigdata_metabase_1        /app/run_metabase.sh             Up      0.0.0.0:13000->3000/tcp
wdpressplus-bigdata_metastore-db_1    docker-entrypoint.sh postgres    Up      5432/tcp
wdpressplus-bigdata_metastore_1       /opt/apache-hive-metastore ...   Up      9083/tcp
wdpressplus-bigdata_minio_1           /usr/bin/docker-entrypoint ...   Up      0.0.0.0:9000->9000/tcp
wdpressplus-bigdata_presto-server_1   /opt/presto-server-0.239/b ...   Up      8080/tcp
```

### Stop and remove containers

```
$ docker-compose stop
Stopping wdpressplus-bigdata_datamart_1      ... done
Stopping wdpressplus-bigdata_metabase_1      ... done
Stopping wdpressplus-bigdata_metastore-db_1  ... done
Stopping wdpressplus-bigdata_metastore_1     ... done
Stopping wdpressplus-bigdata_minio_1         ... done
Stopping wdpressplus-bigdata_presto-server_1 ... done

$ docker-compose rm
Going to remove wdpressplus-bigdata_presto-server_1, wdpressplus-bigdata_datamart_1, wdpressplus-bigdata_metastore_1, wdpressplus-bigdata_metastore-db_1, wdpressplus-bigdata_minio_1, wdpressplus-bigdata_metabase_1
Are you sure? [yN] y
Removing wdpressplus-bigdata_datamart_1      ... done
Removing wdpressplus-bigdata_metabase_1      ... done
Removing wdpressplus-bigdata_metastore-db_1  ... done
Removing wdpressplus-bigdata_metastore_1     ... done
Removing wdpressplus-bigdata_minio_1         ... done
Removing wdpressplus-bigdata_presto-server_1 ... done
```
