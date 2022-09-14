-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.maestro_controles_prd ;
-- MAGIC create table datagov.maestro_controles_prd 
-- MAGIC (
-- MAGIC   id_control string,
-- MAGIC   des_control string,
-- MAGIC   criticidad int,
-- MAGIC   campo string,
-- MAGIC   agrupador_campos string,
-- MAGIC   subdominio string,
-- MAGIC   dominio string,
-- MAGIC   frente string,
-- MAGIC   tabla_calidad string,
-- MAGIC   columnas_excel string,
-- MAGIC   aprobado int
-- MAGIC )
-- MAGIC using delta
-- MAGIC ---partitioned by (frente)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/calidad-de-datos/data/maestro_controles_prd'

-- COMMAND ----------

drop table if exists datagov.maestro_controles_prd ;

-- COMMAND ----------


