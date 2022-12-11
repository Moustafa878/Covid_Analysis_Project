SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.map.java.opts=-Xmx4g;
SET mapreduce.reduce.java.opts=-Xmx4g;

-- creating database covid_db and use it 
CREATE database covid_db;
use covid_db;

-- creating Basic table that we will put csv data in
CREATE  TABLE IF NOT EXISTS covid_db.covid_staging 
(country string,total_cases double,new_cases double,total_deaths double,new_deaths double,total_recovered double,active_cases double,serious double,tot_cases double,deaths double,total_tests double,tests double,cases_per_test double,death_in_closed_cases string,rank_by_testing_rate double,rank_by_death_rate double,rank_by_cases_rate double,rank_by_death_of_closed_cases    double)
ROW FORMAT DELIMITED
FIELDS TERMINATED by '\t'
LOCATION '/user/cloudera/ds/Hive_schema/Basic_Table'
tblproperties ("skip.header.line.count"="1");

-- loading data from Hadoop file COVID_SRC_LZ and put it in covid_staging 
Load data  inpath '/user/cloudera/ds/COVID_SRC_LZ' into table covid_db.covid_staging ;

-- creating table that Dynamically partitioned by Country_Name
CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned(country string,total_cases double,new_cases double,total_deaths double,new_deaths double,total_recovered double,active_cases double,serious double,tot_cases double,deaths double,total_tests double,tests double,cases_per_test double,death_in_closed_cases string,rank_by_testing_rate double,rank_by_death_rate double,rank_by_cases_rate double,rank_by_death_of_closed_cases    double)
PARTITIONED BY (Country_Name string)
STORED as ORC
LOCATION '/user/cloudera/ds/Hive_schema/Partition_Table';

-- seting some properties
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

-- loading data from Basic table to covid_ds_partitioned
FROM
covid_db.covid_staging
INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(Country_Name)
SELECT *,country WHERE Country is not null;

-- creating the first final output Top_Test  that will be displayed in Power BI
INSERT OVERWRITE  DIRECTORY '/user/cloudera/ds/COVID_FINAL_OUTPUT1'
SELECT  country AS Country_Name, total_tests AS Top_Test
FROM covid_db.covid_ds_partitioned
WHERE  country <> 'World'
ORDER BY Top_Test DESC
LIMIT 10;

-- creating the second final output Top_Death  that will be displayed in Power BI
INSERT OVERWRITE  DIRECTORY '/user/cloudera/ds/COVID_FINAL_OUTPUT2'
SELECT  country AS Country_Name, total_deaths AS Top_Death
FROM covid_db.covid_ds_partitioned
WHERE country  <>  'World'
ORDER BY Top_Death DESC
LIMIT 10;
