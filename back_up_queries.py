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
