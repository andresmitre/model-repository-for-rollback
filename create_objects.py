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

    # Crear tablas y vistas en el esquema
    print("Creando tabla SALES_ADVERTISING...")
    create_table_query = """
    CREATE OR REPLACE TABLE REGRESSION_DB.PUBLIC.SALES_ADVERTISING_3 (
        ID INT,
        ADVERTISING_EXPENSE FLOAT,
        SALES FLOAT
    );
    """
    session.sql(create_table_query).collect()
    print("Tabla SALES_ADVERTISING creada")

    print("Creando vista SALES_VIEW...")
    create_view_query = """
    CREATE OR REPLACE VIEW REGRESSION_DB.PUBLIC.SALES_VIEW_3 AS
    SELECT SALE_ID, CUSTOMER_ID, PRODUCT_ID, SALE_DATE, QUANTITY, TOTAL_AMOUNT
    FROM REGRESSION_DB.PUBLIC.SALES;
    """
    session.sql(create_view_query).collect()
    print("Vista SALES_VIEW creada")

    # Crear nuevas features en el feature store (simulado)
    print("Creando nuevas features en el feature store...")
    create_features_query = """
    CREATE OR REPLACE TABLE REGRESSION_DB.PUBLIC.FEATURE_STORE_3 (
        FEATURE_ID INT,
        FEATURE_NAME STRING,
        FEATURE_VALUE FLOAT
    );
    """
    session.sql(create_features_query).collect()
    print("Nuevas features creadas en el feature store")

    # Inserción o actualización en tablas o vistas existentes
    print("Incersión tabla SALES_ADVERTISING_2...")
    insert_update_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING_3 (ID, ADVERTISING_EXPENSE, SALES)
    VALUES (999, 150.00, 250.00)
    """
    session.sql(insert_update_query).collect()
    print("Tabla SALES_ADVERTISING_2 actualizada")

    #print("Insertando datos en SALES_VIEW_2...")
    #insert_query = """
    #INSERT INTO REGRESSION_DB.PUBLIC.SALES_VIEW_2 (SALE_ID, CUSTOMER_ID, PRODUCT_ID, SALE_DATE, QUANTITY, TOTAL_AMOUNT)
    #VALUES (1001, 123, 456, '2024-08-28', 2, 500.00)
    #"""
    #session.sql(insert_query).collect()
    #print("Datos insertados en SALES_VIEW_2")

    print("Incersión en feature store...")
    insert_feature_store_query = """
    INSERT INTO REGRESSION_DB.PUBLIC.FEATURE_STORE_3 (FEATURE_ID, FEATURE_NAME, FEATURE_VALUE)
    VALUES (9, 'New Feature', 999.00)
    """
    session.sql(insert_feature_store_query).collect()
    print("Feature store actualizado")

    # Confirmar la transacción
    print("Confirmando transacción...")
    session.sql("COMMIT;").collect()
    print("Transacción confirmada")
    print("create_objects.py ha terminado.")

except Exception as e:
    # Si ocurre un error, revertir la transacción
    print("Error durante la transacción, iniciando rollback...")
    session.sql("ROLLBACK;").collect()
    print("Transacción revertida debido a un error:", e)

    # Obtener el ID de la última consulta fallida
    query_id = session.sql("SELECT LAST_QUERY_ID();").collect()[0][0]
    print("ID de la consulta fallida:", query_id)
