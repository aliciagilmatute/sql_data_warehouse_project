/*
==================================================================================
			DDL SCRIPT: CREACIÓN DESDE CERO DEL ESQUEMA Y TABLAS
==================================================================================

Objetivo del Script:

	Este script crea las tablas en el schema 'silver',
	borra las tablas existentes.
	Ejecuta este script para re-definir la estructura DDL de las tablas 'silver'
===================================================================================
*/



-- Elimina el esquema completo si existe (incluye todas las tablas)
DROP SCHEMA IF EXISTS silver CASCADE;

-- Crea nuevamente el esquema
CREATE SCHEMA silver;

-- ============================================
-- TABLAS CRM
-- ============================================

CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
	cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost NUMERIC,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales NUMERIC,
    sls_quantity INT,
    sls_price NUMERIC,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- ============================================
-- TABLAS ERP
-- ============================================

CREATE TABLE silver.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE silver.erp_loca101 (
    cid VARCHAR(50),
    cntry VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE silver.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
