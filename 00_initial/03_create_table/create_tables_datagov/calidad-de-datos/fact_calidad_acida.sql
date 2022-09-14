-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fact_calidad_acida ;
-- MAGIC create table datagov.fact_calidad_acida
-- MAGIC (
-- MAGIC    fecha date,
-- MAGIC    id_agrupador_campos string,
-- MAGIC    filtro1 string,
-- MAGIC    filtro2 string,
-- MAGIC    filtro3 string,
-- MAGIC    regs_total int,
-- MAGIC    regs_obs int,
-- MAGIC    regs_ok int
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (fecha)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/calidad-de-datos/data/fact_calidad_acida'

-- COMMAND ----------

drop table if exists datagov.fact_calidad_acida ;
