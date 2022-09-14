-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fletes_maestro_recursos_tms;
-- MAGIC create table datagov.fletes_maestro_recursos_tms
-- MAGIC (
-- MAGIC   rownum int,
-- MAGIC   cod_sociedad string,
-- MAGIC   codigo_recurso string,
-- MAGIC   cod_medio_transporte string,
-- MAGIC   tipo_medio_transporte string,
-- MAGIC   propietario string,
-- MAGIC   flag_bloqueo_proveedor int,
-- MAGIC   placa string,
-- MAGIC   clase_vehiculo string,
-- MAGIC   flag_clase_vehiculo int,
-- MAGIC   capacidad_masa decimal(15,4),
-- MAGIC   unidad_medida_masa string,
-- MAGIC   capacidad_volumen decimal(15,4),
-- MAGIC   unidad_medida_volumen string,
-- MAGIC   capacidad_agregada decimal(15,4),
-- MAGIC   unidad_medida_agregada string,
-- MAGIC   conductor string,
-- MAGIC   tipo_agrupacion_recurso string,
-- MAGIC   atributo_agrupacion_recurso string,
-- MAGIC   dbkey string,
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/fletes_maestro_recursos_tms'

-- COMMAND ----------

select * from datagov.fletes_maestro_recursos_tms

-- COMMAND ----------


