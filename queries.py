from snowflake.snowpark import Session
import os


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

# Depuración de credenciales (evita hacer esto en producción)
print("SF_ACCOUNT:", creds["account"])
print("SF_USER:", creds["user"])
print("SF_PWD:", creds["password"])
print("SF_ROLE:", creds["role"])
print("SF_WAREHOUSE:", creds["warehouse"])
print("SF_DATABASE:", creds["database"])
print("SF_SCHEMA:", creds["schema"])

# Crear una sesión de Snowflake
session = Session.builder.configs(creds).create()
print("Sesión establecida")

# Define tus queries como cadenas
insert_data_query_customers = """
INSERT INTO REGRESSION_DB.PUBLIC.CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, JOIN_DATE) VALUES
(1, 'John Doe', 'john.doe@example.com', '2023-01-01'),
(2, 'Jane Smith', 'jane.smith@example.com', '2023-02-01'),
(3, 'Alice Johnson', 'alice.johnson@example.com', '2023-03-01'),
(4, 'Bob Brown', 'bob.brown@example.com', '2023-04-01'),
(5, 'Charlie Davis', 'charlie.davis@example.com', '2023-05-01');
"""

# Insertar datos en la tabla PRODUCTS
insert_data_query_products = """
INSERT INTO REGRESSION_DB.PUBLIC.PRODUCTS (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE) VALUES
(1, 'Laptop', 'Electronics', 999.99),
(2, 'Smartphone', 'Electronics', 699.99),
(3, 'Tablet', 'Electronics', 399.99),
(4, 'Headphones', 'Accessories', 199.99),
(5, 'Smartwatch', 'Accessories', 299.99);
"""

# Insertar datos en la tabla SALES
insert_data_query_sales = """
INSERT INTO REGRESSION_DB.PUBLIC.SALES (SALE_ID, CUSTOMER_ID, PRODUCT_ID, SALE_DATE, QUANTITY, TOTAL_AMOUNT) VALUES
(1, 1, 1, '2023-01-15', 1, 999.99),
(2, 2, 2, '2023-02-15', 1, 699.99),
(3, 3, 3, '2023-03-15', 1, 399.99),
(4, 4, 4, '2023-04-15', 1, 199.99),
(5, 5, 5, '2023-05-15', 1, 299.99);
"""

# Función para ejecutar los queries dentro de una transacción
def execute_queries():
    try:
        # Construir el comando SQL completo con BEGIN y COMMIT
        sql_command = f"""
        BEGIN;

        {insert_data_query_customers}

        {insert_data_query_products}

        {insert_data_query_sales}

        COMMIT;
        """
        
        # Ejecutar el comando en Snowflake usando la sesión
        session.sql(sql_command).collect()
        print("Transacción completada con éxito.")
    
    except Exception as e:
        # Si ocurre un error, se realiza el rollback
        print("Error durante la ejecución, iniciando rollback...")
        session.sql("ROLLBACK;").collect()
        print(f"Transacción revertida. Error: {e}")

# Ejecutar la función
execute_queries()

# Cerrar la sesión al finalizar
session.close()
print("Sesión cerrada")
