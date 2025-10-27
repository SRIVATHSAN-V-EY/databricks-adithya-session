CREATE OR REPLACE MATERIALIZED VIEW silver.cleansed_sales_data
(
	CONSTRAINT sales_quantity_check EXPECT(quantity is not null) ON VIOLATION DROP ROW,
	CONSTRAINT sales_channel_check EXPECT(channel is not null) ON VIOLATION DROP ROW,
	CONSTRAINT sales_promo_code_check EXPECT(promo_code is not null) ON VIOLATION DROP ROW,
)
AS
select
FS.sale_id,
FS.order_date,
FS.customer_id,
FS.product_id,
FS.quantity,
FS.discount,
FS.region_id,
FS.channel,
FS.promo_code,
c.first_name,
c.last_name,
c.email,
c.join_date,
c.vip,
p.product_name,
p.category,
p.price,
p.in_stock,
r.region_name,
r.country,
FS.quantity * p.price as revenue

from
lakeflow_dlt_uc.sales FS
left join lakeflow_dlt_uc.customers c on FS.customer_id = c.customer_id
left join lakeflow_dlt_uc.products p on FS.product_id = p.product_id
left join lakeflow_dlt_uc.regions r on FS.region_id = r.region_id;