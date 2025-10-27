CREATE OR REPLACE MATERIALIZED VIEW workspace.gold.category_sales_summary 
AS
select
category,
year(order_date) as year_order,
sum(revenue) as total_revenue
from 
workspace.silver.cleansed_sales_data
group by category,year(order_date)
order by category,year_order;

CREATE OR REPLACE MATERIALIZED VIEW workspace.gold.revenue_by_customers_per_region
AS
select * from 
(
select
customer_id,
first_name,
last_name,
region_name,
year(order_date) as year_order,
sum(revenue) as total_revenue,
rank() over(partition by region_name order by sum(revenue) desc) as rnk
from 
workspace.silver.cleansed_sales_data
group by customer,first_name,last_name,region_name,year(order_date)
) ranked_customers
where rnk <= 5
order by region_name,rnk
;


