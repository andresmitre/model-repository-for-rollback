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

    # Primera inserción sin errores en SALES_ADVERTISING
    print("Realizando primera inserción en SALES_ADVERTISING...")
    insert_data_query_1 = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES) VALUES
    (55, 100.00, 200.00);
    """
    session.sql(insert_data_query_1).collect()
    print("Primera inserción realizada en SALES_ADVERTISING")

    # Inserción en la tabla SALES
    print("Realizando inserción en SALES...")
    insert_data_query_3 = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES (SALE_ID, CUSTOMER_ID, PRODUCT_ID, SALE_DATE, QUANTITY, TOTAL_AMOUNT) VALUES
    (1, 101, 202, '2024-08-27', 2, 150.00);
    """
    session.sql(insert_data_query_3).collect()
    print("Inserción realizada en SALES")

    # Segunda inserción con un error deliberado en SALES_ADVERTISING
    print("Realizando segunda inserción en SALES_ADVERTISING con error deliberado...")
    insert_data_query_2 = """
    INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES, NON_EXISTENT_COLUMN) VALUES
    (55, 300.00, 400.00, 'error');
    """
    session.sql(insert_data_query_2).collect()
    print("Segunda inserción realizada en SALES_ADVERTISING")

    # Confirmar la transacción
    print("Confirmando transacción...")
    session.sql("COMMIT;").collect()
    print("Transacción confirmada")

except Exception as e:
    # Si ocurre un error, revertir la transacción
    print("Error durante la transacción, iniciando rollback...")
    session.sql("ROLLBACK;").collect()
    print("Transacción revertida debido a un error:", e)

    # Obtener el ID de la última consulta fallida
    query_id = session.sql("SELECT LAST_QUERY_ID();").collect()[0][0]
    print("ID de la consulta fallida:", query_id)
