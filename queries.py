import os
import logging
from snowflake.snowpark import Session

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Obtener las credenciales de Snowflake desde los secretos configurados
creds = {
    "account": os.getenv('SF_ACCOUNT'),
    "user": os.getenv('SF_USER'),
    "password": os.getenv('SF_PWD'),
    "role": os.getenv('SF_ROLE'),
    "warehouse": os.getenv('SF_WAREHOUSE'),
    "database": os.getenv('SF_DATABASE'),
    "schema": os.getenv('SF_SCHEMA')
}

# Validar que las credenciales no sean None
for key, value in creds.items():
    if value is None:
        logging.error(f"Missing environment variable for {key}.")
        raise ValueError(f"Missing environment variable for {key}.")

# Crear una sesión de Snowflake
try:
    session = Session.builder.configs(creds).create()
    logging.info("Sesión establecida con Snowflake")
except Exception as e:
    logging.critical(f"Error al establecer la sesión con Snowflake: {e}")
    raise

# Define tus queries como cadenas
insert_data_query = """
INSERT INTO REGRESSION_DB.PUBLIC.SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES) VALUES
(10, 100.00, 200.00),
(20, 150.00, 250.00),
(30, 200.00, 300.00),
(40, 250.00, 350.00),
(50, 300.00, 400.00);
"""
# Función para ejecutar los queries dentro de una transacción
def execute_queries():
    try:
        sql_command = f"""
        BEGIN;

        {insert_data_query}

        COMMIT;
        """
        
        session.sql(sql_command).collect()
        logging.info("Transacción completada con éxito.")
    
    except Exception as e:
        logging.error("Error durante la ejecución, iniciando rollback...")
        session.sql("ROLLBACK;").collect()
        logging.error(f"Transacción revertida. Error: {e}")

# Ejecutar la función
execute_queries()

# Cerrar la sesión al finalizar
session.close()
logging.info("Sesión cerrada")
