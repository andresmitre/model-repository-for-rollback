from snowflake.snowpark import Session
import os

# Obtener las credenciales de Snowflake desde los secretos configurados
creds = {
  "account"   : os.getenv('SF_ACCOUNT_2'),
  "user"      : os.getenv('SF_USER_2'),
  "password"  : os.getenv('SF_PWD_2'),
  "role"      : os.getenv('SF_ROLE_2'),
  "warehouse" : os.getenv('SF_WAREHOUSE_2'),
  "database"  : os.getenv('SF_DATABASE_2'),
  "schema"    : os.getenv('SF_SCHEMA_2')
}

# Crear una sesión de Snowflake
session = Session.builder.configs(creds).create()

try:
    # Iniciar la transacción
    print("Iniciando transacción...")
    session.sql("BEGIN;").collect()
    print("Transacción iniciada")

    print("Consultando la tabla feature store con filtro...")
    query_feature_store = """
    SELECT * FROM DB_MLLIVE.LIVE_FS.BANANA_FV$V1
    WHERE ID = 1
    """
    result = session.sql(query_feature_store).collect()
    for row in result:
        print(row)
    print("Consulta completada")

    # Confirmar la transacción
    print("Confirmando transacción...")
    session.sql("COMMIT;").collect()
    print("Transacción confirmada")
    print("Script ha terminado.")

except Exception as e:
    # Si ocurre un error, revertir la transacción
    print("Error durante la transacción, iniciando rollback...")
    session.sql("ROLLBACK;").collect()
    print("Transacción revertida debido a un error:", e)

    # Obtener el ID de la última consulta fallida
    query_id = session.sql("SELECT LAST_QUERY_ID();").collect()[0][0]
    print("ID de la consulta fallida:", query_id)
