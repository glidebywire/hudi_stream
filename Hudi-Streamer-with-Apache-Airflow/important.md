# Essential commands

## wget load those jars
```
ls -h
aws-java-sdk-bundle-1.12.262.jar  hudi-spark3.5-bundle_2.12-1.0.2.jar
hadoop-aws-3.3.4.jar              postgresql-42.7.7.jar
hadoop-common-3.3.4.jar          
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

docker exec -it trino trino --execute "SHOW CATALOGS;"
docker exec -it trino trino --catalog hudi --execute "SHOW SCHEMAS;"
docker exec -it trino trino --catalog hudi --schema default --execute "SHOW TABLES;"

to enter interactive promprt :
```
docker exec -it trino trino --server trino:8080 --catalog hive --schema default
```
then :

```sql
-- List tables in the default database
SHOW TABLES;

-- Check your Hudi table
DESCRIBE coupons_geo;

-- Query some data
SELECT * FROM coupons_geo LIMIT 10;

-- Count rows
SELECT COUNT(*) FROM coupons_geo;
```