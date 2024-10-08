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

# Crear una sesión de Snowflake
session = Session.builder.configs(creds).create()

try:
    # Iniciar la transacción
    print("Iniciando transacción...")
    session.sql("BEGIN;").collect()
    print("Transacción iniciada")

    # Inserción sencilla de la tabla sales_adv...
    insert_update_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES)
    VALUES (101, 200.00, 300.00)
    """
    session.sql(insert_update_query).collect()
    print("Tabla SALES_ADVERTISING insersección")

    # Inserción o actualización en tablas o vistas existentes
    #print("Actualizando tabla SALES_ADVERTISING...")
    #insert_update_query = """
    #INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES)
    #VALUES (100, 150.00, 250.00)
    #ON CONFLICT(ID) DO UPDATE
    #SET ADVERTISING_EXPENSE = EXCLUDED.ADVERTISING_EXPENSE, SALES = EXCLUDED.SALES;
    #"""

    # Inserción de datos en la tabla FEATURE_STORE
    print("Insertando datos en la tabla FEATURE_STORE...")
    insert_feature_store_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.FEATURE_STORE (FEATURE_ID, FEATURE_NAME, FEATURE_VALUE)
    VALUES (2, 'Another Feature', 400.00)
    """
    session.sql(insert_feature_store_query).collect()
    print("Datos insertados en la feature store")

    # Reemplazo de todos los datos en la tabla SALES_ADVERTISING
    print("Reemplazando todos los datos en la tabla SALES_ADVERTISING...")
    truncate_query = "TRUNCATE TABLE REGRESSION_DB.PUBLIC.SALES_ADVERTISING"
    session.sql(truncate_query).collect()

    insert_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES)
    VALUES (999, 200.00, 300.00)
    """
    session.sql(insert_query).collect()
    print("Datos insertados en la tabla SALES_ADVERTISING")


    # Reemplazo de todos los datos en la tabla FEATURE_STORE
    print("Reemplazando todos los datos en la tabla FEATURE_STORE...")
    truncate_feature_store_query = "TRUNCATE TABLE REGRESSION_DB.PUBLIC.FEATURE_STORE"
    session.sql(truncate_feature_store_query).collect()

    insert_feature_store_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.FEATURE_STORE (FEATURE_ID, FEATURE_NAME, FEATURE_VALUE)
    VALUES (2, 'Another Feature', 999.00)
    """
    session.sql(insert_feature_store_query).collect()
    print("Datos insertados en la tabla FEATURE_STORE")



    #print("Actualizando feature store...")
    #insert_feature_store_query = """
    #INSERT INTO REGRESSION_DB.PUBLIC.FEATURE_STORE (FEATURE_ID, FEATURE_NAME, FEATURE_VALUE)
    #VALUES (1, 'New Feature', 300.00)
    #ON CONFLICT(FEATURE_ID) DO UPDATE
    #SET FEATURE_NAME = EXCLUDED.FEATURE_NAME, FEATURE_VALUE = EXCLUDED.FEATURE_VALUE;
    #"""
    #session.sql(insert_feature_store_query).collect()
    #print("Feature store actualizado")

    # Confirmar la transacción
    print("Confirmando transacción...")
    session.sql("COMMIT;").collect()
    print("Transacción confirmada")
    print("Inserciones.py ha terminado.")

except Exception as e:
    # Si ocurre un error, revertir la transacción
    print("Error durante la transacción, iniciando rollback...")
    session.sql("ROLLBACK;").collect()
    print("Transacción revertida debido a un error:", e)

    # Obtener el ID de la última consulta fallida
    query_id = session.sql("SELECT LAST_QUERY_ID();").collect()[0][0]
    print("ID de la consulta fallida:", query_id)
