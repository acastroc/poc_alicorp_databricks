-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fletes_maestro_geocoordenadas_tms;
-- MAGIC create table datagov.fletes_maestro_geocoordenadas_tms
-- MAGIC (
-- MAGIC   rownum int,
-- MAGIC   cliente string COMMENT "Codigo del Cliente" ,
-- MAGIC   nombre string COMMENT "Razon social " ,
-- MAGIC   xpos decimal(15,4) COMMENT "Coordenada X" ,
-- MAGIC   ypos decimal(15,4) COMMENT "Coodenada Y" ,
-- MAGIC   pais_geocoordenada string,
-- MAGIC   dbkey string
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/fletes_maestro_geocoordenadas_tms'

-- COMMAND ----------



-- COMMAND ----------


