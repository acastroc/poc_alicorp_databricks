-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.registros_observados_stg ;
-- MAGIC create table datagov.registros_observados_stg 
-- MAGIC (
-- MAGIC   frente string,
-- MAGIC   fecha_proceso date,
-- MAGIC   id_control string,
-- MAGIC   rownum int,
-- MAGIC   cod_sociedad string,
-- MAGIC   filtro1 string,
-- MAGIC   filtro2 string,
-- MAGIC   filtro3 string,
-- MAGIC   dbkey string
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (frente)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/calidad-de-datos/data/registros_observados_stg'

-- COMMAND ----------

drop table if exists datagov.registros_observados_stg ;

-- COMMAND ----------


