CREATE TABLE workspace.stg.test_table
AS
SELECT * FROM workspace.warehouse.products
WHERE 
product_name= :prod_para