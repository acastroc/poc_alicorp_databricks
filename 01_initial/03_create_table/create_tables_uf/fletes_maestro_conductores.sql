-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.fletes_maestro_conductores ;
-- MAGIC create table uf.fletes_maestro_conductores
-- MAGIC (
-- MAGIC   conductor string COMMENT "Código asignado a un conductor " ,
-- MAGIC   apellido string COMMENT "Apellido del conductor" ,
-- MAGIC   nombre string COMMENT "Nombre del Conductor" ,
-- MAGIC   fecha_inicio_validez string COMMENT "Fecha desde cuando inicia la vigencia del conductor" ,
-- MAGIC   fecha_fin_validez string COMMENT "Fecha que limita la vigencia del conductor" ,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   YEAR_MONTH_DAY string,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (YEAR_MONTH_DAY)
-- MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_maestro_conductores'

-- COMMAND ----------

select * from uf.fletes_maestro_conductores
