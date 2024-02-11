--- create a external table 

create or replace external table dtc-de-411905.week3_dataset_01.external_green_taxi_2022_02
options(
	format = 'parquet',
	uris = ['gs://dtc-de-411905-week3/green_taxi_data_2022_02/832ba01cee0649eea1c96be9afe6ffa5-0.parquet']
);

--- create non partitioned table from the external table

create or replace table dtc-de-411905.week3_dataset_01.green_tripdata_non_partitioned_02 as 
select * from dtc-de-411905.week3_dataset_01.external_green_taxi_2022_02;

--- create partitioned table from the external table 

create or replace table dtc-de-411905.week3_dataset_01.green_tripdata_partitioned_clustered_02
partition by date(lpep_pickup_datetime) 
cluster by pulocationid 
as select * from dtc-de-411905.week3_dataset_01.external_green_taxi_2022_02;

--- count rows with fare_amount = 0

select count(1)
from dtc-de-411905.week3_dataset_01.green_tripdata_non_partitioned_02
where fare_amount = 0;

--- find distinct pulocationid from non partitioned table 

select distinct(pulocationid) 
from dtc-de-411905.week3_dataset_01.green_tripdata_non_partitioned_02
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

--- find distinct pulocationid from partitioned table 

select distinct(pulocationid) 
from dtc-de-411905.week3_dataset_01.green_tripdata_partitioned_clustered_02
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';