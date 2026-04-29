/*
==================================================================================
			DDL SCRIPT: CREACIÓN DESDE CERO DEL ESQUEMA Y TABLAS
==================================================================================

Objetivo del Script:

	Este script crea las tablas en el schema 'bronze',
	borra las tablas existentes.
	Ejecuta este script para re-definir la estructura DDL de las tablas 'bronze'
===================================================================================
*/



-- Elimina el esquema completo si existe (incluye todas las tablas)
DROP SCHEMA IF EXISTS bronze CASCADE;

-- Crea nuevamente el esquema
CREATE SCHEMA bronze;

-- ============================================
-- TABLAS CRM
-- ============================================

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost NUMERIC,
    prd_line VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt TIMESTAMP
);

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales NUMERIC,
    sls_quantity INT,
    sls_price NUMERIC
);

-- ============================================
-- TABLAS ERP
-- ============================================

CREATE TABLE bronze.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

CREATE TABLE bronze.erp_loca101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

