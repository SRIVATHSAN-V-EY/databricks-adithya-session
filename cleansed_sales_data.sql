CREATE OR REPLACE MATERIALIZED VIEW worksapce.silver.cleansed_sales_data
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
workspace.sales FS
left join workspace.customers c on FS.customer_id = c.customer_id
left join workspace.products p on FS.product_id = p.product_id
left join workspace.regions r on FS.region_id = r.region_id;
