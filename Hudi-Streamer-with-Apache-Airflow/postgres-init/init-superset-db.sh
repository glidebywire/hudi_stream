    
#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER superset WITH PASSWORD 'superset';
    CREATE DATABASE superset_db;
    GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset;
EOSQL

  