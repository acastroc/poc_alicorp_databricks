-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE datagov.maestro_conductores_tms (
-- MAGIC   rownum int,
-- MAGIC   cod_sociedad string,
-- MAGIC   brevete string,
-- MAGIC   nombre_conductor string,
-- MAGIC   fecha_inicio_validez date,
-- MAGIC   fecha_fin_validez date,
-- MAGIC   dbkey string
-- MAGIC ) 
-- MAGIC using delta
-- MAGIC LOCATION '/mnt/data_governance/fletes/maestro_conductores_tms'

-- COMMAND ----------

select
		row_number()over(order by conductor) as rownum,
		'PE11' as cod_sociedad,
		conductor as brevete,
		nombre_conductor,
		fecha_inicio_validez,
		fecha_fin_validez,
		conductor as dbkey
	from datagov.fletes_maestro_conductores_raw limit 10

-- COMMAND ----------

with base_maestro_conductores  as (  
select 
		row_number()over(order by conductor) as rownum,
		'PE11' as cod_sociedad,
		conductor as brevete,
		nombre_conductor,
		fecha_inicio_validez,
		fecha_fin_validez,
		conductor as dbkey
	from datagov.fletes_maestro_conductores_raw 
  
 )

insert into datagov.maestro_conductores_tms 
select 
  *
from base_maestro_conductores

-- COMMAND ----------

select * from datagov.maestro_conductores_tms
