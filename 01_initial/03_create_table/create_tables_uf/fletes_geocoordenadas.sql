-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.fletes_geocoordenadas;
-- MAGIC create table uf.fletes_geocoordenadas
-- MAGIC (
-- MAGIC   cliente string COMMENT "Codigo del Cliente" ,
-- MAGIC   nombre string COMMENT "Razon social " ,
-- MAGIC   xpos string COMMENT "Coordenada X" ,
-- MAGIC   ypos string COMMENT "Coodenada Y" ,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   YEAR_MONTH_DAY string,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (YEAR_MONTH_DAY)
-- MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_geocoordenadas'

-- COMMAND ----------


