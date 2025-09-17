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