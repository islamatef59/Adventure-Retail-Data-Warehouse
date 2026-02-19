SELECT * FROM bronze.crm_cust_info

-- check if there are dupicated id 
SELECT cst_id,
COUNT(*)
FROM bronze.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) >1


--check for unwanted spaces 
SELECT cst_firstname
FROM bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)

-- check for consistency in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info 

-- Check for consistency in marital status 
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info 

/*  main query  */
INSERT INTO silver.crm_cust_info(

cst_id ,
cst_key ,
cst_firstname ,
cst_lastname ,
cst_marital_status ,
cst_gndr ,
cst_create_date 

)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE  WHEN UPPER(cst_marital_status)='S' THEN 'Single'
      WHEN UPPER(cst_marital_status)='M' THEN 'Married'
	  ELSE 'n/a'
END cst_marital_status,    -- Normalize marital status to readable format 
CASE WHEN UPPER(cst_gndr)='M' THEN 'Male'
     WHEN UPPER(cst_gndr)='F'THEN 'Female'
	 ELSE 'n/a'
END cst_gndr,               -- Normalize Gender to readable format 
cst_create_date

FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info 
WHERE cst_id IS NOT NULL
) t WHERE flag_last =1    -- Removing dunlicates and choose most recent record 


-- Make Transformation for second table 
-- check if there are dupicated id 
Select * from bronze.crm_prd_info
SELECT prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1
-- Validate that cat_id in silver crm are in bronze erp to make joins later
SELECT REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id 
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_')   NOT IN 
(SELECT DISTINCT id from bronze.erp_px_cat_g1v2)

-- Validate that prd_key match sls_prd_key  in sales _details
SELECT prd_key FROM bronze.crm_prd_info WHERE SUBSTRING(prd_key,7,len(prd_key))  NOT IN 
(SELECT sls_prd_key FROM bronze.crm_sale_details )

-- check for unwanted spaces 
SELECT * FROM bronze.crm_prd_info
WHERE prd_name != TRIM(prd_name)

-- check for negative  or nulls in prd_cost
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- check for invalid date orders 
SELECT prd_start_date,prd_end_date
from bronze.crm_prd_info
where prd_start_date >prd_end_date     

-- check unwanted spaces for sls_orders
SELECT * FROM bronze.crm_sale_details WHERE
sls_ord_num != trim (sls_ord_num)

-- check for prd key in silver.crm_prd_info
SELECT sls_prd_key FROM bronze.crm_sale_details  WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

  --check sls_cust_id not in crm_cust_info
SELECT sls_cust_id FROM bronze.crm_sale_details WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- check for invalid dates 
SELECT 
NULLIF (sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sale_details
WHERE  len(sls_ship_dt) !=8
OR sls_ship_dt < 0
OR sls_ship_dt >20500101
OR sls_ship_dt <19000101

--check for invalid order date 
SELECT sls_order_dt 
FROM bronze.crm_sale_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- check data consistency between sales and quantity and price 
--sales = quantity* price 
-- note values must not be null ,zero,negative 

select sls_sales,
sls_quantity,
sls_price
from bronze.crm_sale_details
where sls_sales != sls_price*sls_quantity
or sls_sales is null 
or sls_price is null 
or sls_quantity is null 
or sls_sales <=0
or sls_quantity <=0
or sls_price <=0

--check is cid match cst_key in crm_cust_info
SELECT 
cid
FROM bronze.erp_CUST_AZ12 
where case when cid like 'NAS%' then SUBSTRING (cid,4,len(cid))
	 else cid
end  not in (select cst_key from silver.crm_cust_info)

-- ccheck birth date for bronze.erp_CUST_AZ12
select bdate
from bronze.erp_CUST_AZ12
where bdate < '1-1-1925' or bdate >GETDATE()

-- check for gender in bronze.erp_CUST_AZ12
select  distinct gen 
from bronze.erp_CUST_AZ12

-- check silver.erp_CUST_AZ12
select distinct gen from silver.erp_CUST_AZ12
select bdate > GETDATE() from silver.erp_CUST_AZ12

--check that cid valuse match cst_key to be ready for joning later 

select 
REPLACE (cid,'-','') cid,
cntry 
from bronze.erp_loc_a101 where REPLACE (cid,'-','')  not in (select cst_key from silver.crm_cust_info)

--check statndardization and consostency for cntry column
select distinct 
case when trim(cntry) in ('US','USA','United States') then 'United States'
     when trim(cntry)='DE' then 'Germany'
	 when cntry=''  or cntry is null then 'n/a'
	 else trim(cntry)
end as cntry
from bronze.erp_loc_a101 



--check for unwanted spaces in  bronze.erp_px_cat_g1v2
select * from bronze.erp_px_cat_g1v2
where cat !=trim(cat) or subcat !=trim(subcat)   or maintenance !=trim(maintenance)

--check for data consistency and  
select distinct 
maintenance 
from
bronze.erp_px_cat_g1v2