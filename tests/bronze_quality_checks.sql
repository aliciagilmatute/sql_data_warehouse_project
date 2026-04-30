/*
TABLA cust_info
*/

SELECT
	*
FROM bronze.crm_cust_info;

-- Revisar NULLS o Duplicates en la Primary Key
-- Expectation: No Result

SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

-- Revisar los espacios no deseados
-- TRIM() elimina los espacios en blanco al principio y al final de una cadena.
-- Si el valor de: WHERE cst_firstname != TRIM(cst_firstname); No coincide = TENEMOS UN PROBLEMA
-- Expectation: No Results

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization and consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info;

-- Les damos nombres completos



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
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;


-- Separar prd_key en cat_id y prd_key para unirlo despues con la tabla bronze.erp_px_cat_g1v2

-- Revisar los espacios no deseados
-- TRIM() elimina los espacios en blanco al principio y al final de una cadena.
-- Si el valor de: WHERE cst_firstname != TRIM(cst_firstname); No coincide = TENEMOS UN PROBLEMA
-- Expectation: No Results

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Revisar NULLS, Duplicates o Valores Negativos de las columnas numericas: prd_cost
-- Expectation: No Results

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0;

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL; -- TENEMOS NULLS (SOLUCIONAMOS CON COALESCE)


-- Data Standardization and consistency: prd_line

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Les damos nombres completos

-- Comprobamos que las fechas esten correctas

SELECT * FROM bronze.crm_prd_info
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
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');


/* 

TABLA: crm_sales_details

*/

-- Revisamos que no haya espacios indeseados
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_quantity, 
	sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num!=TRIM(sls_ord_num);

-- Vamos a comprobar que la PK coincide con los id's de las tablas 
-- con las que la vamos a unir

-- Tabla: silver.crm_prd_info
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_quantity, 
	sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info); --IMPORTANTE: con la tabla de la capa silver: que es la prd_info que esta limpia

-- Tabla: silver.crm_cust_info

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity, 
	sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info); 

-- Vamos a comprobar que no haya fechas no válidas

SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0;

-- Hay muchos casos con sls_order_dt=0, vamos a sustituirlos por NULLS

SELECT
	NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0;


-- Vamos a transformar las variables de fecha que estan en formato INT

SELECT
	NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0 
OR LENGTH(sls_order_dt::text)!=8  -- 4 caracteres(año)+ 2 caracteres (mes)+2 caracteres(dia)
OR sls_order_dt>20500101
OR sls_order_dt <19000101; 


SELECT
	NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt<=0 
OR LENGTH(sls_ship_dt::text)!=8  -- 4 caracteres(año)+ 2 caracteres (mes)+2 caracteres(dia)
OR sls_ship_dt>20500101
OR sls_ship_dt <19000101; 


SELECT
	NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt<=0 
OR LENGTH(sls_due_dt::text)!=8  -- 4 caracteres(año)+ 2 caracteres (mes)+2 caracteres(dia)
OR sls_due_dt>20500101
OR sls_due_dt <19000101; 

-- Hay que comprobar que sls_order_dt < sls_ship_dt
-- Expectativa: No Result

SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Revisamos la consistencia de los datos: sales, quantity and price
-- >>> Sales = Quantity*Price
-- >>> NO DEBE HABER VALORES NULL, CERO O NEGATIVOS

-- no cumple regla?:
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price;

-- valores NULL?:
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL;

-- valores negativos o 0?:
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0;


-- Todas las comprobraciones a la vez:

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price ASC;

-- sls_sales: tenemos NULLS, VALORES NEGATIVOS Y CEROS
-- sls_quantity: parece que esta bien
-- sls_price: tenemos NULLS, valores negativos
-- No se cumple la regla en algunos casos: (EJEMPLO: 50=2*50)


SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,

	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity*ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,

	CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN ROUND(sls_sales / NULLIF(sls_quantity,0))
		ELSE sls_price
	END AS sls_price
	
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price ASC;


/*
TABLA: erp_cust_az12
*/

SELECT
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000';

SELECT * FROM silver.crm_cust_info;

-- Por lo visto en erp_cust_az12 la variable cid comienza con NAS
-- En la tabla crm_cust_info no aparece
-- Tenemos que transformar cid para poder conectarlas

-- Comprobacion de que ahora coincida con silver.crm_cust_info:
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid::text))
		ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid::text))
		ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);



-- Revisamos que no haya fechas fuera del rango normal:

SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > CURRENT_DATE;

-- Estandarizas/Normalizar la variable gen

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

-- Tenemos celdas vacias, nulls, F/M, etc...



/*
TABLA: erp_loc_a101
*/

SELECT
	cid,
	cntry
FROM bronze.erp_loca101;  -- Tiene guion en cid


SELECT cst_key FROM silver.crm_cust_info; -- No tiene guion en cid

-- Lo reemplazamos y lo comprobamos en silver.crm_cust_info:
-- Expectativa: NO RESULT

SELECT
	REPLACE(cid,'-','') AS cid,
	cntry
FROM bronze.erp_loca101
WHERE REPLACE(cid,'-','') NOT IN
(SELECT cst_key FROM silver.crm_cust_info);


-- Estandarizacion de la variable cntry:

SELECT DISTINCT
	cntry AS old_cntry,
	CASE WHEN TRIM(cntry)= 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loca101
ORDER BY cntry;



/*
TABLA: erp_px_cat_g1v2
*/

SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT cat_id FROM silver.crm_prd_info;

-- Revisar que no hay espacios indeseados:

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat!=TRIM(cat);


SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat!=TRIM(subcat);

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance!=TRIM(maintenance);


-- Todo en una sola query:

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat!=TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);


-- Revisar la estandarizacion de las variables

SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2;

-- ESTA TABLA TIENE DATOS DE BUENA CALIDAD Y NO HAY QUE LIMPIAR NADA

