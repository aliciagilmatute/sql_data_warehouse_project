/*
==================================================================================================================
					STORED PROCEDURE: CARGA MASIVA DE DATOS PARA PGADMIN. ETL PROCESS
==================================================================================================================

Objetivo del Script:
	Este stored procedure carga los datos en el schema 'bronze' desde archivos CSV externos.
	Realiza los siguientes pasos:
	
		- 'Vacia' (Trunca) las tablas del schema bronze antes de cargar los datos.
		- Usa la función 'COPY' para insertar los datos desde los archivos csv a las tablas del schema bronze. 

Parámetros:
	Ninguno. 
	Este stored procedure no acepta ningún parámetro ni devuelve ningún valor.

Ejemplo:
	CALL bronze.load_bronze;

------------------------------------------------------------------------------------------------------------------
IMPORTANTE:
	Los archivos deben estar en una carpeta accesible por PostgreSQL
	Ejemplo recomendado: C:/temp/
==================================================================================================================

*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '======================';

    -------------------------------------------------------------------
    RAISE NOTICE '----------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '----------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE 'Loading: bronze.crm_cust_info';

    COPY bronze.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
    )
    FROM 'C:/Users/alici/Desktop/SQL/mi_datawarehouse_project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE 'Loading: bronze.crm_prd_info';

    COPY bronze.crm_prd_info (
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    FROM 'C:/Users/alici/Desktop/SQL/mi_datawarehouse_project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE 'Loading: bronze.crm_sales_details';

    COPY bronze.crm_sales_details (
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
    FROM 'C:/Users/alici/Desktop/SQL/mi_datawarehouse_project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    RAISE NOTICE '----------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '----------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE 'Loading: bronze.erp_cust_az12';

    COPY bronze.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    FROM 'C:/Users/alici/Desktop/SQL/mi_datawarehouse_project/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: bronze.erp_loca101';
    TRUNCATE TABLE bronze.erp_loca101;

    RAISE NOTICE 'Loading: bronze.erp_loca101';

    COPY bronze.erp_loca101 (
        cid,
        cntry
    )
    FROM 'C:/Users/alici/Desktop/SQL/mi_datawarehouse_project/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE 'Loading: bronze.erp_px_cat_g1v2';

    COPY bronze.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    FROM 'C:/Users/alici/Desktop/SQL/mi_datawarehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Load Duration: % seconds', duration;
    RAISE NOTICE '--------------------------------------------------';

    -------------------------------------------------------------------
    batch_end_time := clock_timestamp();
    batch_duration := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Load Bronze Layer Duration: % seconds', batch_duration;
    RAISE NOTICE '========================================================';

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_message = MESSAGE_TEXT,
        v_state   = RETURNED_SQLSTATE;

    RAISE NOTICE '=========================================';
    RAISE NOTICE 'ERROR DURING BRONZE LOAD';
    RAISE NOTICE 'Message: %', v_message;
    RAISE NOTICE 'State: %', v_state;
    RAISE NOTICE '=========================================';
END;
$$;
