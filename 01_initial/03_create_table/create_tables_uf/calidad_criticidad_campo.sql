-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.calidad_criticidad_campo;
-- MAGIC create table uf.calidad_criticidad_campo
-- MAGIC (
-- MAGIC   frente string ,
-- MAGIC   dominio string ,
-- MAGIC   subdominio string ,
-- MAGIC   agrupador_campo string,
-- MAGIC   campo string,
-- MAGIC   criticidad decimal(15,2),
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC LOCATION '/mnt/data_user_file/user-file/calidad_criticidad_campo'

-- COMMAND ----------

drop table uf.calidad_criticidad_campo
