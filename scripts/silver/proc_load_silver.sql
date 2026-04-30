/*
==================================================================================================================
                STORED PROCEDURE: CARGA MASIVA DE DATOS PARA PGADMIN - SILVER ETL PROCESS
==================================================================================================================

Objetivo del Script:
    Este stored procedure carga y transforma los datos desde el schema 'bronze' hacia el schema 'silver'.

Descripción del Proceso:
    El procedimiento realiza un proceso ETL (Extract, Transform, Load) sobre las tablas del nivel Silver,
    aplicando reglas de limpieza, estandarización y control de calidad.

Pasos Principales:
    1. Trunca (vacía) las tablas del schema 'silver' antes de cada carga para garantizar una recarga completa.
    2. Extrae los datos desde las tablas del schema 'bronze'.
    3. Aplica transformaciones y validaciones, incluyendo:
        - Eliminación de duplicados
        - Normalización de valores categóricos (género, estado civil, país, etc.)
        - Limpieza de espacios en blanco
        - Corrección de fechas inválidas
        - Recalculo de métricas inconsistentes (ventas, precios)
        - Gestión de valores nulos o incorrectos
    4. Inserta los datos transformados en las tablas del schema 'silver'.
    5. Registra tiempos de ejecución por tabla y duración total del proceso.
    6. Implementa manejo de errores mediante bloque EXCEPTION para facilitar debugging y monitoreo.

Tablas Procesadas:
    - silver.crm_cust_info
    - silver.crm_prd_info
    - silver.crm_sales_details
    - silver.erp_cust_az12
    - silver.erp_loca101
    - silver.erp_px_cat_g1v2

Características Técnicas:
    - Lenguaje: PL/pgSQL
    - Control de tiempos con clock_timestamp()
    - Monitoreo mediante RAISE NOTICE
    - Manejo de errores con GET STACKED DIAGNOSTICS

Notas:
    - Este procedimiento depende de que las tablas del schema 'bronze' ya estén cargadas previamente.
    - Diseñado para ejecutarse desde pgAdmin o mediante:
          CALL silver.load_silver();

Autor:
    Alicia Gil Matute

Fecha:
    30/04/2026
==================================================================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	v_message TEXT;
    v_state   TEXT;

    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    duration   NUMERIC;

    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
    batch_duration    NUMERIC;
	
BEGIN

    batch_start_time := clock_timestamp();

    RAISE NOTICE '======================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '======================';

    -------------------------------------------------------------------
    RAISE NOTICE '----------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '----------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_material_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

	end_time := clock_timestamp();
	duration := EXTRACT(EPOCH FROM (end_time - start_time));
	
	RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';


    -------------------------------------------------------------------
	start_time := clock_timestamp();
	
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_dt::DATE AS prd_start_dt,
        (
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - INTERVAL '1 day'
        )::DATE AS prd_end_dt
    FROM bronze.crm_prd_info;

	end_time := clock_timestamp();
	duration := EXTRACT(EPOCH FROM (end_time-start_time));
	
	RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();
	
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL
                 OR sls_sales <= 0
                 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN ROUND(sls_sales / NULLIF(sls_quantity, 0))
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

	end_time := clock_timestamp();
	duration := EXTRACT(EPOCH FROM (end_time-start_time));
	
    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    RAISE NOTICE '----------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '----------------------';
	
    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

	end_time := clock_timestamp();
	duration := EXTRACT(EPOCH FROM (end_time-start_time));
	
	RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';


    -------------------------------------------------------------------
    start_time := clock_timestamp();
	
    RAISE NOTICE '>> Truncating Table: silver.erp_loca101';
    TRUNCATE TABLE silver.erp_loca101;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_loca101';
    INSERT INTO silver.erp_loca101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loca101;

	end_time := clock_timestamp();
	duration := EXTRACT(EPOCH FROM (end_time-start_time));
	
    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

	
    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

	end_time := clock_timestamp();
	duration := EXTRACT(EPOCH FROM(end_time-start_time));

	RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    batch_end_time := clock_timestamp();
    batch_duration := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Load Silver Layer Duration: % seconds', batch_duration;
    RAISE NOTICE '========================================================';

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_message = MESSAGE_TEXT,
        v_state   = RETURNED_SQLSTATE;

    RAISE NOTICE '=========================================';
    RAISE NOTICE 'ERROR DURING SILVER LOAD';
    RAISE NOTICE 'Message: %', v_message;
    RAISE NOTICE 'State: %', v_state;
    RAISE NOTICE '=========================================';

END;
$$;
