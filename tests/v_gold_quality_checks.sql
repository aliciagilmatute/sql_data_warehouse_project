/*
===================================================================================================
						FINAL VALIDATION CHECKS - GOLD LAYER VIEWS
===================================================================================================
Objetivo:
    Validar que las vistas/tablas finales de negocio (dim_customers, dim_products, fact_sales)
    estén correctamente construidas y que mantengan integridad referencial.

Comprobaciones:
    - Revisión general de dimensiones (clientes y productos)
    - Validación de dominios (ej. género)
    - Revisión de hechos (ventas)
    - Integridad referencial entre FACT y DIMENSIONS
    - Detección de duplicados en claves de negocio

Notas:
    - Ejecutar tras completar la carga de GOLD
    - Cualquier discrepancia debe analizarse y resolverse antes de explotación analítica
===================================================================================================
*/


-- ==================================================================
-- gold.dim_customers (crm_cust_info +erp_cust_az12 + erp_loca101 )
-- ==================================================================

SELECT * FROM gold.dim_customers;

SELECT DISTINCT gender FROM gold.dim_customers;


-- ==================================================================
-- gold.dim_products (crm_prd_info + erp_px_cat_g1v2)
-- ==================================================================

SELECT * FROM gold.dim_products;


-- ==================================================================
-- gold.fact_sales  (crm_sales_details)
-- ==================================================================

SELECT * FROM gold.fact_sales;

-- Un checkeo que se suele hacer con las tablas FACTS es conectarlas 
-- con las tablas DIMENSION para localizar posibles errores (Foreign Key Integrity):
--Expectativa: NO RESULT

SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;  -- No matching, al ser un LEFT JOIN debe haber NULLS si no coinciden

	
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
WHERE p.product_key IS NULL;


