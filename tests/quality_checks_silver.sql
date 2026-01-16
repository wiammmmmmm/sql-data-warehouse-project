/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- checking cleaned crm_cust_info in silver
select * from Silver.crm_cust_info

--1 check PK must be unique and not null 
-- expectation : no result

select cst_id, count(*) from Silver.crm_cust_info
group by cst_id having count(*) > 1 or  cst_id is null

--2 check for unwanted spaces 
select cst_firstname
from Silver.crm_cust_info 
where cst_firstname != trim(cst_firstname)

--3 check data consistency 
--data standardization

select distinct cst_gndr
from Silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

select * from silver.crm_prd_info

--1 check PK must be unique and not null 
-- expectation : no result

select prd_id, count(*) from silver.crm_prd_info
group by prd_id having count(*) > 1 or prd_id is null

--2 check for unwanted spaces 
select prd_nm
from silver.crm_prd_info 
where prd_nm != trim(prd_nm)

-- check nulls and negative numbers
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null


--3 check data consistency 
--data standardization

select distinct prd_line
from silver.crm_prd_info

-- check for invalid date orders
-- end date must not be earlier than the start date
select  *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt
-- solution => end date = start of th next record -1

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- check for invalid dates
--check for outliers by validating the boundries od date range
select 
nullif(sls_due_dt,0) sls_due_dt
from Silver.crm_sales_details
where len(sls_due_dt) !=8 
or sls_due_dt > 20500101
or sls_due_dt < 10000101
or sls_due_dt <= 0

-- check for invalid date orders
select * from Silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


select * from Silver.crm_sales_details

-- check data consistency: between sales, qte, and price
-- business rule : sales = qte * price
--  values must not be null, zero, or negative

select distinct  sls_quantity, 
sls_sales , sls_price

from Silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity, sls_price 


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- check birth date
-- identify out of range dates

select distinct 
bdate 
from Silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

-- data standardization & consistency

select distinct  
gen
from Silver.erp_cust_az12

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
select distinct
    cntry 
from silver.erp_loc_a101
order by cntry

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check for Unwanted Spaces

select
    * 
from silver.erp_px_cat_g1v2
where cat != trim(cat) 
   OR subcat != trim(subcat) 
   OR maintenance != trim(maintenance);

-- Data Standardization & Consistency
select distinct 
    maintenance 
from silver.erp_px_cat_g1v2;
