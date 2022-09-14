-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.calidad_criticidad_dominio;
-- MAGIC create table uf.calidad_criticidad_dominio
-- MAGIC (
-- MAGIC   frente string ,
-- MAGIC   dominio string ,
-- MAGIC   criticidad decimal(15,2),
-- MAGIC   columnas_necesarias string,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC LOCATION '/mnt/data_user_file/user-file/calidad_criticidad_dominio'

-- COMMAND ----------

drop table uf.calidad_criticidad_dominio
