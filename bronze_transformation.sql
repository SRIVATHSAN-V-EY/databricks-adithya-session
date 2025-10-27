bronze_transformation.sql

CREATE STREAMING LIVE TABLE bronze.fact_sales
AS
select * from cloud_files(
'/Volumes/lakeflow_dlt_uc/landing_zone/fact_and_dimensions_files/fact_sales',
'csv',
map('header','true')
);

CREATE STREAMING LIVE TABLE bronze.dim_products
AS
select * from cloud_files(
'/Volumes/lakeflow_dlt_uc/landing_zone/fact_and_dimensions_files/dim_products',
'csv',
map('header','true')
);

CREATE STREAMING LIVE TABLE bronze.dim_customers
AS
select * from cloud_files(
'/Volumes/lakeflow_dlt_uc/landing_zone/fact_and_dimensions_files/dim_customers',
'csv',
map('header','true')
);

CREATE STREAMING LIVE TABLE bronze.dim_regions
AS
select * from cloud_files(
'/Volumes/lakeflow_dlt_uc/landing_zone/fact_and_dimensions_files/dim_regions',
'csv',
map('header','true')
);

