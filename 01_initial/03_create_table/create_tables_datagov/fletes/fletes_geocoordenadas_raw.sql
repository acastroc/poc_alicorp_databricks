-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fletes_geocoordenadas_raw;
-- MAGIC create table datagov.fletes_geocoordenadas_raw
-- MAGIC (
-- MAGIC   cliente string COMMENT "Codigo del Cliente" ,
-- MAGIC   nombre string COMMENT "Razon social " ,
-- MAGIC   xpos decimal(15,4) COMMENT "Coordenada X" ,
-- MAGIC   ypos decimal(15,4) COMMENT "Coodenada Y" 
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/fletes_geocoordenadas_raw'

-- COMMAND ----------

with base_geocoordenadas  as (  
select 
		cliente,
        nombre,
        cast(replace(xpos,',','') as decimal(15,4)) as xpos,
        cast(replace(ypos,',','') as decimal(15,4)) as ypos

	from uf.fletes_geocoordenadas 
  
 )

insert into datagov.fletes_geocoordenadas_raw 
select 
  *
from base_geocoordenadas

-- COMMAND ----------

select 
		cliente,
        nombre,
        cast(replace(xpos,',','') as decimal(15,4)) as xpos,
        cast(replace(ypos,',','') as decimal(15,4)) as ypos

	from uf.fletes_geocoordenadas 
