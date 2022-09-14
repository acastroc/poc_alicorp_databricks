-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fletes_maestro_conductores_tms;
-- MAGIC create table datagov.fletes_maestro_conductores_tms
-- MAGIC (
-- MAGIC   rownum int,
-- MAGIC   cod_sociedad string,
-- MAGIC   brevete string,
-- MAGIC   nombre_conductor string,
-- MAGIC   fecha_inicio_validez date,
-- MAGIC   fecha_fin_validez date,
-- MAGIC   dbkey string
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/fletes_maestro_conductores_tms'

-- COMMAND ----------

select * from datagov.fletes_maestro_conductores_tms

-- COMMAND ----------


