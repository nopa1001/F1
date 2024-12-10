-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.secrets.get("f1-scope", "f1datalkey")
-- MAGIC f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.circuits;
CREATE TABLE IF NOT EXISTS f1_processed.circuits(circuit_id INT,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
altitude DOUBLE,
ingestion_date TIMESTAMP
)
USING parquet
OPTIONS (path "abfss://processed@f1datal.dfs.core.windows.net/circuits/", header true)

-- COMMAND ----------

SELECT * FROM f1_processed.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.races;
CREATE TABLE IF NOT EXISTS f1_processed.races(race_id INT,
race_year INT,
round INT,
circuit_id INT,
name STRING,
infestion_date TIMESTAMP,
race_timestamp TIMESTAMP)
USING parquet
OPTIONS (path "abfss://processed@f1datal.dfs.core.windows.net/races/", header true)

-- COMMAND ----------

select * from f1_processed.races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.constructors;
CREATE TABLE IF NOT EXISTS f1_processed.constructors(
constructor_id INT,
constructor_ref STRING,
name STRING,
nationality STRING,
ingestion_date TIMESTAMP)
USING parquet
OPTIONS(path "abfss://processed@f1datal.dfs.core.windows.net/constructors/")

-- COMMAND ----------

select * from f1_processed.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.drivers;
CREATE TABLE IF NOT EXISTS f1_processed.drivers(
driver_id INT,
driver_ref STRING,
number INT,
code STRING,
name STRING,
dob DATE,
nationality STRING,
ingestion_date TIMESTAMP)
USING parquet
OPTIONS (path "abfss://processed@f1datal.dfs.core.windows.net/drivers/")

-- COMMAND ----------

select * from f1_processed.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.results;
CREATE TABLE IF NOT EXISTS f1_processed.results(
result_id INT,
driver_id INT,
constructor_id INT,
number INT,grid INT,
position INT,
position_text STRING,
position_order INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastest_lap INT,
rank INT,
fastest_lap_time STRING,
fastest_lap_spead FLOAT,
ingestion_date TIMESTAMP,
race_id STRING)
USING parquet
OPTIONS(path "abfss://processed@f1datal.dfs.core.windows.net/results/")

-- COMMAND ----------

select * from f1_processed.results;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.pit_stops;
CREATE TABLE IF NOT EXISTS f1_processed.pit_stops(
race_id INT,
driver_id INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT,
ingestion_date TIMESTAMP)
USING parquet
OPTIONS(path "abfss://processed@f1datal.dfs.core.windows.net/pt_stops/")

-- COMMAND ----------

select * from f1_processed.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.lap_times;
CREATE TABLE IF NOT EXISTS f1_processed.lap_times(
race_id INT,
driver_id INT,
lap INT,
position INT,
time STRING,
milliseconds INT,
ingestion_date TIMESTAMP
)
USING parquet
OPTIONS (path "abfss://processed@f1datal.dfs.core.windows.net/laptimes/")

-- COMMAND ----------

select * from f1_processed.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.qualifying;
CREATE TABLE IF NOT EXISTS f1_processed.qualifying(
qualify_id INT,
race_id INT,
driver_id INT,
constructor_id INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
ingestion_date TIMESTAMP)
USING parquet
OPTIONS (path "abfss://processed@f1datal.dfs.core.windows.net/quali/")

-- COMMAND ----------

select * from f1_processed.qualifying
