/* 
=================================================================
DDl Scripts: Create Bronze Tables
=================================================================
Script Purpose:
		This script create tables in the 'bronze' schema, 
		dropping existing tables if they already exists.
		Run this scripts to re-define the DDL structure of 'bronze' tables
==================================================================
*/


if object_id('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(60),
	cst_lastname nvarchar(50),
	cst_maritial_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date nvarchar(50)
);

if object_id('bronze.crm_prd_info', 'U') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_name nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_st_dt nvarchar(50),
	prd_end_dt nvarchar(50)
);

if object_id('bronze.crm_sales_details', 'U') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);


if object_id('bronze.erp_cust_az101', 'U') is not null
	drop table bronze.erp_cust_az101;
create table bronze.erp_cust_az101(
	cid nvarchar(50),
	cntry nvarchar(50)
);


if object_id('bronze.erp_cust_az12', 'U') is not null
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);


if object_id('bronze.erp_px_cat_g1v2', 'U') is not null
	drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintance nvarchar(50)
);

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime
	begin try
		print '=====================================';
		print 'Loading bronze laye';
		print '=====================================';

		print '-------------------------------------';
		print 'Loading CRM data';
		print '-------------------------------------';

		set @start_time = getdate();
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'C:\Users\sarvesh jathar\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '>>> ---------------';

		set @start_time = getdate();
		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info
		from 'C:\Users\sarvesh jathar\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '>>> ---------------';

		set @start_time = getdate();
		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from 'C:\Users\sarvesh jathar\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '>>> ---------------';

		print '-------------------------------------';
		print 'Loading ERP data';
		print '-------------------------------------';


		set @start_time = getdate();
		truncate table bronze.erp_cust_az101;
		bulk insert bronze.erp_cust_az101
		from 'C:\Users\sarvesh jathar\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '>>> ---------------';


		set @start_time = getdate();
		truncate table bronze.erp_cust_az12;
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\sarvesh jathar\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '>>> ---------------';

		set @start_time = getdate();
		truncate table bronze.erp_px_cat_g1v2;
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\sarvesh jathar\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print '>>> ---------------';


	end try
	begin catch
		print '========================';
		print 'bronze data loading fail';
		print '=========================';
	end catch
end

exec bronze.load_bronze;
