/*
=========================================================================================================
                    SCRIPT DE DATA QUALITY / DATA CLEANING — CAPA GOLD
=========================================================================================================
Objetivo general:
    Este script ejecuta controles de calidad y validación sobre las tablas de la capa Silver
    para garantizar consistencia, integridad y estandarización antes de su consumo analítico
    o su promoción a capas posteriores (Gold / Reporting).

Principales validaciones:
    1. Unicidad de las surrogate keys de las tablas DIMENSION.
    2. Integracion referencial entre la tabla FACT y las tablas DIMENSION.
    3. Validacion de las relaciones en data model para objetivos analíticos posteriores.

Notas:
	- Ejecuta este código después de cargar los datos en la capa 'Silver'.
	- Investigar y resolver cualquier discrepancia encontrada durante el proceso de comprobación. 
=========================================================================================================
*/


-- ==================================================================
-- crm_cust_info +erp_cust_az12 + erp_loca101 + 
-- ==================================================================

-- Comprobamos que no hay duplicados.
-- Expectativa: NO RESULT

SELECT cst_id, COUNT(*) FROM
(SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_material_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loca101 la
	ON ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*)>1;

-- Ahora tenemos problemas de integracion: cst_gndr(crm) y gen(erp)
-- Tenemos la misma informacion que proceden de sources diferentes
-- No coinciden a la hora de etiquetar con el mismo genero a la misma persona 

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender Info
		ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loca101 la
	ON ci.cst_key=la.cid
ORDER BY 1,2;

-- Renombramos las columnas a unos nombres mas amigables y con significado:

-- Ordena las columnas en grupos logicos para mejorar la comprension


-- ==================================================================
-- crm_prd_info + erp_px_cat_g1v2
-- ==================================================================

-- Esta tabla recoge tanto la informacion actual como el historico de productos
-- Tenemos que tomar una decicision: quedarnos con la informacion actual o con el historico
-- Vamos a quedarnos con la informacion actual 

-- Para ello filtramos: si prd_end_dt es NULL entonces es informacion actual:
-- (podemos retirar del SELECT la columna pn.prd_end_dt)

SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;  -- Filtramos dejando fuera todo el histórico



-- SELECT * FROM silver.erp_px_cat_g1v2;
-- SELECT * FROM silver.crm_prd_info;



-- Revisamos que prd_key es unica:
-- Expectativa: NO RESULT

SELECT 
	prd_key,
	COUNT(*)
FROM
(SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL  
)t GROUP BY prd_key
HAVING COUNT(*)>1;


-- ==================================================================
-- crm_sales_details 
-- ==================================================================

-- Una tabla FACT debe contener las surrogate keys que hemos creado en las tablas DIMENSIONS
-- porque vamos a conectarlas entre sí a través de las surrogate keys, no a través de los ID's originales
-- Al proceso de traer una columna de una tabla a otra se le llama: Data LookUp

-- Una vez hecho el data lookup podemos retirar del SELECT: sd.sls_prd_key y sd.sls_cust_id.

SELECT
	sd.sls_ord_num,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr  	-- La surrogate key la tenemos en la capa 'gold'          
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id;
