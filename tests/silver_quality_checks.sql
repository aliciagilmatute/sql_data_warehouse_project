/*
TABLA cust_info
*/

-- Revisar NULLS o Duplicates en la Primary Key
-- Expectation: No Result

SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

-- Revisar los espacios no deseados
-- TRIM() elimina los espacios en blanco al principio y al final de una cadena.
-- Si el valor de: WHERE cst_firstname != TRIM(cst_firstname); No coincide = TENEMOS UN PROBLEMA
-- Expectation: No Results

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- Data Standardization and consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info;


SELECT * FROM silver.crm_cust_info;




/*
Tabla prd_info
*/


SELECT
	*
FROM bronze.crm_prd_info;


-- Revisar NULLS o Duplicates en la Primary Key
-- Expectation: No Result

SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;


-- Separar prd_key en cat_id y prd_key para unirlo despues con la tabla bronze.erp_px_cat_g1v2

-- Revisar los espacios no deseados
-- TRIM() elimina los espacios en blanco al principio y al final de una cadena.
-- Si el valor de: WHERE cst_firstname != TRIM(cst_firstname); No coincide = TENEMOS UN PROBLEMA
-- Expectation: No Results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Revisar NULLS, Duplicates o Valores Negativos de las columnas numericas: prd_cost
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0;

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL; -- TENEMOS NULLS (SOLUCIONAMOS CON COALESCE)


-- Data Standardization and consistency: prd_line

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Les damos nombres completos

-- Comprobamos que las fechas esten correctas

SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt<prd_start_dt; -- No tiene sentido que la fecha final sea antes que la de inicio

-- Probamos a corregir con LEAD() en una muestra mas pequeña:
-- los modelos 'AC-HE-HL-U509-R' y 'AC-HE-HL-U509'

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_end_dt)- INTERVAL '1 day' AS prd_end_dt_test  -- (-1) para indicar el dia anterior y que no se solape con el siguiente prd_start_date
FROM silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');


SELECT * FROM silver.crm_prd_info;


/*

TABLA: crm_sales_details

*/

-- Revisar las fecha no validas

SELECT
	*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- Revisar las reglas de las variables sls_sales, sls_quantity, sls_price:

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price ASC;


SELECT * FROM silver.crm_sales_details;

/*
TABLA: erp_cust_az12
*/

-- Identificar fechas fuera del rango normal:

SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate<'1924-01-01' OR bdate>CURRENT_DATE;

-- Normalizacion de la variable gen:

SELECT DISTINCT
	gen
FROM silver.erp_cust_az12;


SELECT * FROM silver.erp_cust_az12;


/*
TABLA: erp_loc_a101
*/

-- Revisar que cid no contiene guion

SELECT 
	cid
FROM silver.erp_loca101;

-- Revisar que la estandarizacion de cntry se ha hecho correctamente

SELECT DISTINCT
	cntry
FROM silver.erp_loca101
ORDER BY cntry;


SELECT * FROM silver.erp_loca101;


/*
TABLA: erp_px_cat_g1v2
*/


SELECT * FROM silver.erp_px_cat_g1v2;



