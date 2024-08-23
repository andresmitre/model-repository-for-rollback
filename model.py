from snowflake.snowpark import Session
import os

# Obtener las credenciales de Snowflake desde los secretos configurados
creds = {
  "account"   : os.getenv('SNOWFLAKE_ACCOUNT'),
  "user"      : os.getenv('SNOWFLAKE_USER'),
  "password"  : os.getenv('SNOWFLAKE_PASSWORD'),
  "role"      : os.getenv('SNOWFLAKE_ROLE'),
  "warehouse" : os.getenv('SNOWFLAKE_WAREHOUSE'),
  "database"  : os.getenv('SNOWFLAKE_DATABASE'),
  "schema"    : os.getenv('SNOWFLAKE_SCHEMA')
}

# Crear una sesión de Snowflake
session = Session.builder.configs(creds).create()

# Crear la tabla de datos de ventas y publicidad
create_data_table = """
CREATE OR REPLACE TABLE SALES_ADVERTISING (
    ID INT,
    ADVERTISING_EXPENSE DECIMAL(10, 2),
    SALES DECIMAL(10, 2)
);
"""
session.sql(create_data_table).collect()

# Insertar datos en la tabla
insert_data_query = """
INSERT INTO SALES_ADVERTISING (ID, ADVERTISING_EXPENSE, SALES) VALUES
    (1, 1000.00, 1500.00),
    (2, 2000.00, 2500.00)
"""
session.sql(insert_data_query).collect()

print("Tabla creada y datos insertados en Snowflake.")
