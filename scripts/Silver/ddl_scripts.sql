--Building silver layer


if object_id('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(60),
	cst_lastname nvarchar(50),
	cst_maritial_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if object_id('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_name nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_st_dt nvarchar(50),
	prd_end_dt nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if object_id('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);


if object_id('silver.erp_cust_az101', 'U') is not null
	drop table silver.erp_cust_az101;
create table silver.erp_cust_az101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()
);


if object_id('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()
);


if object_id('silver.erp_px_cat_g1v2', 'U') is not null
	drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

/* UPDATE bronze.crm_cust_info
SET cst_create_date = CONVERT(DATE, cst_create_date, 103);  
ALTER TABLE bronze.crm_cust_info
ALTER COLUMN cst_create_date DATE; */

--Data Cleansing and transformation
--Check for nulls and duplicates in primary key and removing extra spaces and data standardization and Data Consistency

create or alter procedure silver.load_silver as
begin
	truncate table silver.crm_cust_info
	insert into 
	silver.crm_cust_info(cst_id,
						cst_key,
						cst_firstname,
						cst_lastname,
						cst_maritial_status,
						cst_gndr,
						cst_create_date)
	select cst_id, 
	cst_key, 
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname, 
	case when upper(trim(cst_maritial_status)) = '' then 'Single'
		 when upper(trim(cst_maritial_status)) = 'M' then 'Married'
		 else 'Unknown'
	end as cst_maritial_status,
	case when upper(trim(cst_gndr)) = 'F' then 'Female'
		 when upper(trim(cst_gndr)) = 'M' then 'Male'
		 else 'Unknown'
	end as cst_gndr,
	cst_create_date  from (
	select *,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag
	from bronze.crm_cust_info) t
	where flag = 1

	truncate table silver.crm_prd_info
	insert into silver.crm_prd_info( 
		prd_id,   
		cat_id, 
		prd_key,
		prd_name, 
		prd_cost, 
		prd_line, 
		prd_st_dt, 
		prd_end_dt)
	select prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_name,
	isnull(prd_cost, 0) as prd_cost,
	case when trim(trim(prd_line)) = 'M' then 'Mountains'
		 when trim(trim(prd_line)) = 's' then 'Othe Sales'
		 when trim(trim(prd_line)) = 'R' then 'Roads'
		 when trim(trim(prd_line)) = 'T' then 'Touring'
		 else 'N/A'
		end as prd_line,
	prd_st_dt,
	prd_end_dt
	from bronze.crm_prd_info;

	truncate table silver.crm_sales_details
	insert into silver.crm_sales_details(
		sls_ord_num, 
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_due_dt,
		sls_ship_dt,
		sls_sales,
		sls_quantity,
		sls_price)
	select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 then NULL
			else try_cast(cast(sls_order_dt as varchar(8)) as date)
		end as sls_order_dt,
			case 
			when sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 then NULL
			else try_cast(cast(sls_ship_dt as varchar(8)) as date)
		end as sls_ship_dt,
			case 
			when sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 then NULL
			else try_cast(cast(sls_due_dt as varchar(8)) as date)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
			then sls_quantity * abs(sls_price)
			else sls_sales
			end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity, 0)
		else sls_price
		end as sls_price
	from bronze.crm_sales_details;

	truncate table silver.erp_cust_az12
	insert into silver.erp_cust_az12(
	cid, bdate, gen)
	select 
	case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
	end as cid,
	case when bdate > getdate() then null
	else bdate
	end as bdate,
	case when upper(trim(gen)) = 'M' then 'Male'
	when upper(trim(gen)) = 'F' then 'Female'
	when upper(trim(gen)) is null or ltrim(rtrim(gen)) = '' then 'Unknown'
	else gen
	end as gen
	from bronze.erp_cust_az12;

	truncate table silver.erp_cust_az101
	insert into silver.erp_cust_az101(
	cid, cntry)
	select
	replace(cid, '-', '') as cid,
	case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry) = '' or trim(cntry) is null then 'N/A'
	else trim(cntry)
	end as cntry
	from bronze.erp_cust_az101;

	truncate table silver.erp_px_cat_g1v2
	insert into silver.erp_px_cat_g1v2(
	id, cat, subcat, maintance)
	select id,
	cat,
	subcat,
	maintance from bronze.erp_px_cat_g1v2;
end

exec silver.load_silver;
