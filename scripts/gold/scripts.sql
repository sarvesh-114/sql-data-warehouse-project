
create view gold.dim_customers as 
select
row_number() over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as firstname,
ci.cst_lastname as lastname,
ci.cst_maritial_status as maritial_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
end as gender,
ci.cst_create_date as create_date,
ca.bdate as birth_date,
la.cntry as country
from silver.crm_cust_info as ci 
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_cust_az101 as la
on ci.cst_key = la.cid

drop view if exists gold.dimesion_products;
create view gold.dimesion_products as 
select
row_number() over(order by pn.prd_st_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_name as product_name,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as sub_category,
pc.maintance as maintainance,
pn.prd_st_dt as start_date
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null

select * from gold.dimesion_products;

create view gold.fact_sales as 
select
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price
from silver.crm_sales_details as sd
left join gold.dimesion_products as pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers as cu
on sd.sls_cust_id = cu.customer_id
