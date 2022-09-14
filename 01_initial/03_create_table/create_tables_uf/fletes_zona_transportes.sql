-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.fletes_zona_transportes;
-- MAGIC create table uf.fletes_zona_transportes
-- MAGIC (
-- MAGIC   cliente string COMMENT "Codigo del Cliente" ,
-- MAGIC   zona string COMMENT "Codigo de la Zona " ,
-- MAGIC   descripcion_zona string COMMENT "Descripcion de la Zona" ,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   YEAR_MONTH_DAY string,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_zona_transportes'

-- COMMAND ----------


