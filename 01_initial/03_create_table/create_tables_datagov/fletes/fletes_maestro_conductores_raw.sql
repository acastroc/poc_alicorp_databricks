-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fletes_maestro_conductores_raw;
-- MAGIC create table datagov.fletes_maestro_conductores_raw
-- MAGIC (
-- MAGIC   conductor string,
-- MAGIC   apellido string,
-- MAGIC   nombre string,
-- MAGIC   nombre_conductor string,
-- MAGIC   fecha_inicio_validez date,
-- MAGIC   fecha_fin_validez date
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/fletes_maestro_conductores_raw'

-- COMMAND ----------

select * from datagov.fletes_maestro_conductores_raw

-- COMMAND ----------

with base_conductores  as (  select conductor,
  apellido,
  nombre,
  coalesce(nombre,'') || 
            case when coalesce(apellido,'')='' then '' else ' ' || apellido end as nombre_conductor,
  to_date(to_timestamp(fecha_inicio_validez, "d/M/yyyy")) as fecha_inicio_validez,
  to_date(to_timestamp(fecha_fin_validez, "d/M/yyyy")) as fecha_fin_validez
  from uf.fletes_maestro_conductores)

insert into datagov.fletes_maestro_conductores_raw 
select 
  *.
from base_conductores

-- COMMAND ----------

  select  
  conductor,
  apellido,
  nombre,
  coalesce(nombre,'') || 
			case when coalesce(apellido,'')='' then '' else ' ' || apellido end as nombre_conductor,
  to_date(to_timestamp(fecha_inicio_validez, "d/M/yyyy")) as fecha_inicio_validez,
  to_date(to_timestamp(fecha_fin_validez, "d/M/yyyy")) as fecha_fin_validez
  from uf.fletes_maestro_conductores limit 100
