CREATE STREAMING LIVE TABLE workspace.silver.sales
AS
select
cast(sale_id as INT) as sale_id,
to_date(order_date,'dd/MM/yyyy') as order_date,
cast(customer_id as INT) as customer_id,
cast(product_id as INT) as product_id,
cast(quantity as INT) as quantity,
cast(discount as INT) as discount,
cast(region_id as INT) as region_id,
cast(channel as STRING) as channel,
cast(promo_code as STRING) as promo_code

from stream(workspace.bronze.sales);

CREATE STREAMING LIVE TABLE workspace.silver.customers
AS
select
cast(customer_id as INT) as customer_id,
cast(first_name as STRING) as first_name,
cast(last_name as STRING) as last_name,
cast(email as STRING) as email,
to_date(join_date,'dd/MM/yyyy') as join_date,
cast(vip as STRING) as vip

from stream(workspace.bronze.customers);

CREATE STREAMING LIVE TABLE workspace.silver.products
AS
select
cast(product_id_id as INT) as product_id,
cast(product_name as STRING) as product_name,
cast(category as STRING) as category,
cast(price as INT) as price,
cast(in_stock as INT) as in_stock

from stream(workspace.bronze.products);

CREATE STREAMING LIVE TABLE workspace.silver.regions
AS
select
cast(region_id as INT) as region_id,
cast(region_name as STRING) as region_name,
cast(country as STRING) as country

from stream(workspace.bronze.regions);


