/*
=========================================================================================================
                    SCRIPT DE DATA QUALITY / DATA CLEANING — CAPA SILVER
=========================================================================================================
Objetivo general:
    Este script ejecuta controles de calidad y validación sobre las tablas de la capa Silver
    para garantizar consistencia, integridad y estandarización antes de su consumo analítico
    o su promoción a capas posteriores (Gold / Reporting).

Principales validaciones:
    1. Integridad de Primary Keys (NULLs o duplicados)
    2. Detección de espacios en blanco no deseados
    3. Estandarización y consistencia de valores categóricos
    4. Validación de campos numéricos (NULLs, negativos o inconsistencias)
    5. Coherencia lógica de fechas
    6. Preparación y validación de claves para joins entre tablas

Buenas prácticas:
    - Ejecutar estas comprobaciones tras cada carga o transformación en Silver.
    - Analizar cualquier discrepancia antes de continuar con procesos downstream.
    - Documentar incidencias y acciones correctivas aplicadas.
    - Mantener este script como referencia de auditoría y control de calidad.

Nota:
    La expectativa habitual en la mayoría de checks es “No Results”.
    Si una consulta devuelve registros, implica una anomalía que debe investigarse.
=========================================================================================================
*/


/*
=========================================================================================================
TABLA: crm_cust_info
Descripción:
    Validación de calidad sobre datos maestros de clientes.
=========================================================================================================
*/

-- 1. Revisar NULLs o duplicados en la Primary Key (cst_id)
-- Expectation: No Results

SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- 2. Revisar espacios en blanco no deseados
-- TRIM() elimina espacios al inicio y al final.
-- Si el valor original difiere de TRIM(valor), existe un problema de limpieza.
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


-- 3. Revisar estandarización y consistencia de variables categóricas
-- Objetivo: detectar valores inesperados, inconsistentes o mal normalizados

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info;


-- Vista general de la tabla para inspección manual
SELECT *
FROM silver.crm_cust_info;




/*
=========================================================================================================
TABLA: crm_prd_info
Descripción:
    Validación de calidad sobre catálogo de productos.
=========================================================================================================
*/

-- Vista previa de datos en Bronze para contraste
SELECT *
FROM bronze.crm_prd_info;


-- 1. Revisar NULLs o duplicados en la Primary Key (prd_id)
-- Expectation: No Results

SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- 2. Preparación de prd_key:
-- Separar cat_id y prd_key para facilitar joins posteriores con bronze.erp_px_cat_g1v2


-- 3. Revisar espacios en blanco no deseados en nombres de producto
-- Expectation: No Results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- 4. Revisar campos numéricos (prd_cost)
-- Validar NULLs, duplicados lógicos o valores negativos
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0;

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL;
-- Acción sugerida: imputación o reemplazo mediante COALESCE según reglas de negocio


-- 5. Estandarización de línea de producto (prd_line)

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Acción sugerida:
-- Reemplazar abreviaturas o códigos por nombres descriptivos completos


-- 6. Validación lógica de fechas
-- La fecha de fin no puede ser anterior a la fecha de inicio

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- 7. Test de corrección con LEAD()
-- Ajuste de fechas para evitar solapamientos entre versiones del mismo producto

SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (
        PARTITION BY prd_key
        ORDER BY prd_end_dt
    ) - INTERVAL '1 day' AS prd_end_dt_test
FROM silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


-- Vista general
SELECT *
FROM silver.crm_prd_info;




/*
=========================================================================================================
TABLA: crm_sales_details
Descripción:
    Validación de calidad sobre detalle transaccional de ventas.
=========================================================================================================
*/

-- 1. Revisar inconsistencias entre fechas de pedido, envío y vencimiento
-- Regla: order_date <= ship_date <= due_date

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


-- 2. Validar reglas de negocio en métricas comerciales
-- Regla esperada:
-- sls_sales = sls_quantity * sls_price
-- Además, no deben existir NULLs ni valores <= 0

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price ASC;


-- Vista general
SELECT *
FROM silver.crm_sales_details;




/*
=========================================================================================================
TABLA: erp_cust_az12
Descripción:
    Validación de datos maestros ERP de clientes.
=========================================================================================================
*/

-- 1. Identificar fechas de nacimiento fuera de rango lógico
-- Regla:
-- No anteriores a 1924-01-01
-- No futuras respecto a CURRENT_DATE

SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURRENT_DATE;


-- 2. Normalización de género

SELECT DISTINCT
    gen
FROM silver.erp_cust_az12;


-- Vista general
SELECT *
FROM silver.erp_cust_az12;




/*
=========================================================================================================
TABLA: erp_loca101
Descripción:
    Validación de localización y país.
=========================================================================================================
*/

-- 1. Revisar formato de cid
-- Regla esperada: sin guiones o caracteres no deseados

SELECT
    cid
FROM silver.erp_loca101;


-- 2. Revisar correcta estandarización de país (cntry)

SELECT DISTINCT
    cntry
FROM silver.erp_loca101
ORDER BY cntry;


-- Vista general
SELECT *
FROM silver.erp_loca101;




/*
=========================================================================================================
TABLA: erp_px_cat_g1v2
Descripción:
    Tabla de categorías de producto utilizada para enriquecimiento y joins.
=========================================================================================================
*/

-- Validación exploratoria general

SELECT *
FROM silver.erp_px_cat_g1v2;


