-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.fletes_maestro_recursos_raw;
-- MAGIC create table datagov.fletes_maestro_recursos_raw
-- MAGIC (
-- MAGIC   recurso string,
-- MAGIC   medio_transporte string,
-- MAGIC   propietario string,
-- MAGIC   placa string,
-- MAGIC   clase_vehiculo string,
-- MAGIC   dimension_1 string,
-- MAGIC   capacidad_1 decimal(15,4),
-- MAGIC   unidad_medida_1 string,
-- MAGIC   dimension_2 string,
-- MAGIC   capacidad_2 decimal(15,4),
-- MAGIC   unidad_medida_2 string,
-- MAGIC   dimension_3 string,
-- MAGIC   capacidad_3 decimal(15,4),
-- MAGIC   unidad_medida_3 string,
-- MAGIC   dimension_4 string,
-- MAGIC   capacidad_4 decimal(15,4),
-- MAGIC   conductor_estandar string,
-- MAGIC   tipo_agrupacion_recurso string,
-- MAGIC   atributo_agrupacion_recurso string
-- MAGIC )
-- MAGIC using delta
-- MAGIC --partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/fletes_maestro_recursos_raw'

-- COMMAND ----------

select * from datagov.fletes_maestro_recursos_raw

-- COMMAND ----------

with base_maestro_recursos as (
  select 
    f.recurso ,
    f.medio_transporte ,
    f.propietario ,
    f.placa ,
    f.clase_vehiculo ,
    f.dimension_1 ,
    cast(replace(f.capacidad_1,',','') as decimal(15,4)) as capacidad_1,
    f.unidad_medida_1 ,
    f.dimension_2 ,
    cast(replace(f.capacidad_2,',','') as decimal(15,4)) as capacidad_2,
    f.unidad_medida_2 ,
    f.dimension_3 ,
    cast(replace(f.capacidad_3,',','') as decimal(15,4)) as capacidad_3,
    f.unidad_medida_3 ,
    f.dimension_4 ,
    cast(replace(f.capacidad_4,',','') as decimal(15,4)) as capacidad_4,
    f.conductor_estandar ,
    f.tipo_agrupacion_recurso ,
    f.atributo_agrupacion_recurso 
  from uf.fletes_maestro_recursos f
  
)
insert into datagov.fletes_maestro_recursos_raw 
select 
  *
from base_maestro_recursos

-- COMMAND ----------

select 
  f.recurso ,
  f.medio_transporte ,
  f.propietario ,
  f.placa ,
  f.clase_vehiculo ,
  f.dimension_1 ,
  cast(replace(f.capacidad_1,',','') as decimal(15,4)) as capacidad_1,
  f.unidad_medida_1 ,
  f.dimension_2 ,
  cast(replace(f.capacidad_2,',','') as decimal(15,4)) as capacidad_2,
  f.unidad_medida_2 ,
  f.dimension_3 ,
  cast(replace(f.capacidad_3,',','') as decimal(15,4)) as capacidad_3,
  f.unidad_medida_3 ,
  f.dimension_4 ,
  cast(replace(f.capacidad_4,',','') as decimal(15,4)) as capacidad_4,
  f.conductor_estandar ,
  f.tipo_agrupacion_recurso ,
  f.atributo_agrupacion_recurso 
  from uf.fletes_maestro_recursos f limit 100

-- COMMAND ----------

select * from datagov.fletes_maestro_recursos_raw limit 100;
