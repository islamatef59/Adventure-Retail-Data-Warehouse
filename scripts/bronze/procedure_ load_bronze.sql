CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time	DATETIME, @end_time DATETIME,@batch_start_time DATETIME ,@batch_end_time DATETIME
	BEGIN TRY
	SET @batch_start_time=GETDATE();
		 PRINT'==========================================================';
		 PRINT'Loading Bronze Layer '
		 PRINT'==========================================================';

		  PRINT'---------------------------------------------------------';
		  PRINT'Loading CRM Tables '
		  PRINT'---------------------------------------------------------';

			  PRINT'>>>>>>>>  bronze.crm_cust_info   >>>>>>>>';
			  SET @start_time=GETDATE();
			TRUNCATE TABLE bronze.crm_cust_info
			BULK INSERT bronze.crm_cust_info
			 from 'D:\Data engineer\courses\data warehouse project course photos\PROOOJECT\data warehouse project\datasets\source_crm\cust_info.csv'

			 WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			 )
			   PRINT'>>>>>>>>  bronze.crm_prd_info   >>>>>>>>';
			   SET @end_time=GETDATE();
			   PRINT 'Load duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
            SET @start_time=GETDATE();
			TRUNCATE TABLE bronze.crm_prd_info
			BULK INSERT bronze.crm_prd_info
			 from 'D:\Data engineer\courses\data warehouse project course photos\PROOOJECT\data warehouse project\datasets\source_crm\prd_info.csv'

			 WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			 )
			   SET @end_time=GETDATE();
			   PRINT 'Load duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
		  PRINT'>>>>>>>>  bronze.crm_sale_details    >>>>>>>>';
		    SET @start_time=GETDATE();
			TRUNCATE TABLE bronze.crm_sale_details
			BULK INSERT bronze.crm_sale_details
			 from 'D:\Data engineer\courses\data warehouse project course photos\PROOOJECT\data warehouse project\datasets\source_crm\sales_details.csv'

			 WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			 )
			   SET @end_time=GETDATE();
			   PRINT 'Load duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
		  PRINT'---------------------------------------------------------';
		  PRINT'Loading ERP Tables '
		  PRINT'---------------------------------------------------------';

		  PRINT'>>>>>>>>   Truncting table bronze.erp_CUST_AZ12    >>>>>>>>';
		   SET @start_time=GETDATE();
			TRUNCATE TABLE bronze.erp_CUST_AZ12
			BULK INSERT bronze.erp_CUST_AZ12
			 from 'D:\Data engineer\courses\data warehouse project course photos\PROOOJECT\data warehouse project\datasets\source_erp\CUST_AZ12.csv'

			 WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			 )
			   SET @end_time=GETDATE();
			   PRINT 'Load duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
		  PRINT'>>>>>>>>   Truncting table bronze.erp_loc_a1012    >>>>>>>>';
		   SET @start_time=GETDATE();
			TRUNCATE TABLE bronze.erp_loc_a101
			BULK INSERT bronze.erp_loc_a101
			 from 'D:\Data engineer\courses\data warehouse project course photos\PROOOJECT\data warehouse project\datasets\source_erp\LOC_A101.csv'

			 WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			 )
			   SET @end_time=GETDATE();
			   PRINT 'Load duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
			   PRINT '----------------------'
		  PRINT'>>>>>>>>   bronze.erp_px_cat_g1v2    >>>>>>>>';
		   SET @start_time=GETDATE();
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			 BULK INSERT bronze.erp_px_cat_g1v2
			 from 'D:\Data engineer\courses\data warehouse project course photos\PROOOJECT\data warehouse project\datasets\source_erp\PX_CAT_G1V2.csv'

			 WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			 )
			   SET @end_time=GETDATE();
			   PRINT 'Load duration : '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
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
