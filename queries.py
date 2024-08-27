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
insert_data_query_customers = """
INSERT INTO REGRESSION_DB.PUBLIC.CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, JOIN_DATE) VALUES
(1, 'John Doe', 'john.doe@example.com', '2023-01-01'),
(2, 'Jane Smith', 'jane.smith@example.com', '2023-02-01'),
(3, 'Alice Johnson', 'alice.johnson@example.com', '2023-03-01'),
(4, 'Bob Brown', 'bob.brown@example.com', '2023-04-01'),
(5, 'Charlie Davis', 'charlie.davis@example.com', '2023-05-01');
"""
# Función para ejecutar los queries dentro de una transacción
def execute_queries():
    try:
        sql_command = f"""
        BEGIN;

        {insert_data_query_customers}

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
