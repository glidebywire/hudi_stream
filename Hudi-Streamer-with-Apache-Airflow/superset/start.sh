#!/bin/bash

export SUPERSET_DATABASE_URI=postgresql://superset:superset@metastore-db:5432/superset_db

pip install psycopg2-binary

/app/.venv/bin/superset db upgrade --database-uri $SUPERSET_DATABASE_URI
/app/.venv/bin/superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin --database-uri $SUPERSET_DATABASE_URI
/app/.venv/bin/superset init --database-uri $SUPERSET_DATABASE_URI
/app/.venv/bin/superset run -h 0.0.0.0 -p 8088 --database-uri $SUPERSET_DATABASE_URI
