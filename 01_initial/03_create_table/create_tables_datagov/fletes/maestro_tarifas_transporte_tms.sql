-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.maestro_tarifas_transporte_tms;
-- MAGIC create table datagov.maestro_tarifas_transporte_tms
-- MAGIC (
-- MAGIC   rownum double,
-- MAGIC   cod_sociedad string,
-- MAGIC   cod_proveedor string,
-- MAGIC   razon_social string ,
-- MAGIC   flag_proveedor_s4 int,
-- MAGIC   flag_transportista_s4 int,
-- MAGIC   cod_zona_expedicion string,
-- MAGIC   des_zona_expedicion string,
-- MAGIC   cod_zona_destino string,
-- MAGIC   des_zona_destino string,
-- MAGIC   tabla_tarifas string,
-- MAGIC   tipo_tarifa string ,
-- MAGIC   tipo_unidad string,
-- MAGIC   capacidad decimal(15,4),
-- MAGIC   grupo_porte_prod string,
-- MAGIC   tarifa decimal(15,4),
-- MAGIC   tarifa_media decimal(15,4),
-- MAGIC   tarifa_min decimal(15,4),
-- MAGIC   num_tarifas_similares int,
-- MAGIC   inicio_validez date,
-- MAGIC   fin_validez date,
-- MAGIC   status_proveedor string,
-- MAGIC   flag_zona_expedicion int,
-- MAGIC   flag_zona_destino int,
-- MAGIC   grupo_porte_prod2 string,
-- MAGIC   tarifa2 decimal(15,4),
-- MAGIC   var_tarifa_media float,
-- MAGIC   var_tarifa_min float,
-- MAGIC   dbkey string
-- MAGIC )
-- MAGIC using delta
-- MAGIC ---partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/maestro_tarifas_transporte_tms'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC drop table if exists datagov.maestro_tarifas_transporte_tms;

-- COMMAND ----------


