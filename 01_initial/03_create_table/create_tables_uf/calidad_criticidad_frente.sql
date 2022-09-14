-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.calidad_criticidad_frente;
-- MAGIC create table uf.calidad_criticidad_frente
-- MAGIC (
-- MAGIC   frente string,
-- MAGIC   tipo_frente string,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC LOCATION '/mnt/data_user_file/user-file/calidad_criticidad_frente'

-- COMMAND ----------

drop table uf.calidad_criticidad_frente
