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

print("sesion establecida")

try:
    # Iniciar la transacción
    session.sql("BEGIN;").collect()
    print("Transacción iniciada")

    # Insertar datos en la tabla con un error deliberado
    insert_data_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES, NON_EXISTENT_COLUMN) VALUES
    (55, 100.00, 200.00, 'error'),
    (55, 300.00, 400.00, 'error');
    """
    session.sql(insert_data_query).collect()
    print("Datos insertados")

    # Confirmar la transacción
    session.sql("COMMIT;").collect()
    print("Transacción confirmada")

except Exception as e:
    # Si ocurre un error, revertir la transacción
    session.sql("ROLLBACK;").collect()
    print("Error durante la transacción, se ha revertido:", e)
