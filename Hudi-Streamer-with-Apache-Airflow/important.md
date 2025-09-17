# Essential commands

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