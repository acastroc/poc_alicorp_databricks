-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.fletes_status_proveedor;
-- MAGIC create table uf.fletes_status_proveedor
-- MAGIC (
-- MAGIC   cod_proveedor string COMMENT "Codigo del proveedor" ,
-- MAGIC   descripcion string COMMENT "Nombre del proveedor " ,
-- MAGIC   status string COMMENT "fase del proveedor" ,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_status_proveedor'

-- COMMAND ----------

drop table uf.fletes_status_proveedor
