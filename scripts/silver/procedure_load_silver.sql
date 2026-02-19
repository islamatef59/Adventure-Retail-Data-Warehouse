CREATE OR ALTER PROCEDURE silver.load_server AS
BEGIN 
  DECLARE @start_time	DATETIME, @end_time DATETIME,@batch_start_time DATETIME ,@batch_end_time DATETIME

		BEGIN TRY
SET @batch_start_time=GETDATE();
		 PRINT'==========================================================';
		 PRINT'Loading Silver Layer '
		 PRINT'==========================================================';

		  PRINT'---------------------------------------------------------';
		  PRINT'Loading CRM Tables '
		  PRINT'---------------------------------------------------------';
	print' >>>>>>  Truncating and insert table ssilver.crm_prd_info >>>>>>>> '
			  SET @start_time=GETDATE();
	INSERT INTO silver.crm_prd_info(
	prd_id ,
	cat_id ,
	prd_key ,
	prd_name,
	prd_cost ,
	prd_line ,
	prd_start_date,
	prd_end_date


	)
	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) AS prd_Pkey,
	prd_name,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE  UPPER(TRIM(prd_line))
		  WHEN  'M' THEN 'Mountain'
		  WHEN  'R' THEN 'Road'
		  WHEN 'S' THEN 'other sales'
		  WHEN  'T' THEN 'Touring'
		  ELSE 'n/a'
	END AS prd_lien,
	CAST (prd_start_date AS DATE) AS prd_start_date ,
	CAST (DATEADD(DAY,-1, LEAD (prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date)) AS DATE)AS prd_end_date                                
	FROM bronze.crm_prd_info
	 SET @end_time=GETDATE();
			   PRINT 'operation duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'

	print' >>>>>>  Truncating and insert table silver.crm_sale_details  >>>>>>>> '
	
	SET @start_time=GETDATE();
	INSERT INTO  silver.crm_sale_details(
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



	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt=0 OR LEN (sls_order_dt) !=8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt=0 OR LEN (sls_ship_dt) !=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LEN (sls_due_dt) !=8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price) THEN  sls_quantity * ABS (sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
		 else sls_price 
	end as sls_price 
	FROM bronze.crm_sale_details 

	SET @end_time=GETDATE();
			   PRINT 'operation duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
   
	 PRINT'---------------------------------------------------------';
	 PRINT'Loading ERP Tables '
	 PRINT'---------------------------------------------------------';
	 print' >>>>>>  Truncating and insert table silver.erp_CUST_AZ12  >>>>>>>> '
	
	
	SET @start_time=GETDATE();
	insert into silver.erp_CUST_AZ12(
	cid,
	 bdate,
	 gen
	)

	SELECT 
	case when cid like 'NAS%' then SUBSTRING (cid,4,len(cid))
		 else cid
	end as cid ,
	case when bdate > GETDATE() THEN NULL 
		 ELSE bdate
	end as bdate,
	case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		 when upper(trim(gen)) in ('M','MALE') then 'MALE'
		 else 'n/a'
	end as gen
	FROM bronze.erp_CUST_AZ12

	SET @end_time=GETDATE();
			   PRINT 'operation duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'


	print' >>>>>>  Truncating and insert table silver.erp_loc_a101  >>>>>>>> '

	SET @start_time=GETDATE();
	insert into silver.erp_loc_a101 (
	cid ,
	cntry
	)
	select 
	REPLACE (cid,'-','') cid,
	 case when trim(cntry) in ('US','USA','United States') then 'United States'
		 when trim(cntry)='DE' then 'Germany'
		 when cntry=''  or cntry is null then 'n/a'
		 else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101 

	SET @end_time=GETDATE();
			   PRINT 'operation duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
	
	SET @start_time=GETDATE();
	print' >>>>>>  Truncating and insert table silver.erp_px_cat_g1v2  >>>>>>>> '
	insert into silver.erp_px_cat_g1v2(
	 id ,
	 cat ,
	 subcat,
	 maintenance
	)
	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2

	SET @end_time=GETDATE();
			   PRINT 'operation duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
END TRY
			 BEGIN CATCH
			 PRINT'=======================================';
			 PRINT'Error Occured during Loading Bronz Layer';
			 PRINT 'Error Message '+ERROR_MESSAGE();
			 PRINT 'Error Message '+CAST(ERROR_NUMBER() AS NVARCHAR);
			 PRINT 'Error Message '+CAST(ERROR_STATE() AS NVARCHAR);
			 PRINT'=======================================';
			 END CATCH
       SET @batch_end_time=GETDATE();
	   PRINT 'The Total Time for Loading process is '+ CAST(DATEDIFF(second ,@batch_start_time, @batch_end_time) AS NVARCHAR)+ ' seconds' 

END
GO
exec silver.load_server
