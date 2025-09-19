# Essential commands

## wget load those jars
```
ls -h
aws-java-sdk-bundle-1.12.262.jar  hudi-spark3.5-bundle_2.12-1.0.2.jar  woodstox-core-5.4.0.jar
hadoop-aws-3.3.4.jar              postgresql-42.7.7.jar
hadoop-common-3.3.4.jar           stax2-api-4.2.1.jar
```

## test metastore table init

```
docker-compose run --rm hive-metastore \
  schematool -info \
  -dbType postgres \
  -userName hive \
  -passWord hive \
  -url jdbc:postgresql://metastore-db:5432/metastore_db
```

or query hive table trough sqltools in vscode
```
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;```
```

on connection     Connection Name: Hive Metastore DB (or any name you like)

    Connection Method: Server and Port

    Server Address: localhost

    Server Port: 5432

    Database: metastore_db

    Username: hive

    Password: hive