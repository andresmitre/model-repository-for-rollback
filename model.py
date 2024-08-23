from snowflake.snowpark import Session
import os

# Obtener las credenciales de Snowflake desde los secretos configurados
creds = {
  "account"   : os.getenv('SF_ACCOUNT'),
  "user"      : os.getenv('SF_USER'),
  "password"  : os.getenv('SF_PWD'),
  "role"      : os.getenv('SF_ROLE'),
  "warehouse" : os.getenv('SF_WAREHOUSE'),
  "database"  : os.getenv('SF_DATABASE'),
  "schema"    : os.getenv('SF_SCHEMA')
}

# Imprimir las credenciales para depuración
print("SF_ACCOUNT:", creds["account"])
print("SF_USER:", creds["user"])
print("SF_PWD:", creds["password"])
print("SF_ROLE:", creds["role"])
print("SF_WAREHOUSE:", creds["warehouse"])
print("SF_DATABASE:", creds["database"])
print("SF_SCHEMA:", creds["schema"])

# Crear una sesión de Snowflake
session = Session.builder.configs(creds).create()