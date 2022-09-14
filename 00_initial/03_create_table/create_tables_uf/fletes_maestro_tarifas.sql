-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists uf.fletes_maestro_tarifas;
-- MAGIC create table uf.fletes_maestro_tarifas
-- MAGIC (
-- MAGIC   cod_proveedor string COMMENT "Codigo del Proveedor" ,
-- MAGIC   razon_social string COMMENT "Razon social del Proveedor" ,
-- MAGIC   cod_zona_origen string COMMENT "Codigo de la Zona de Origen" ,
-- MAGIC   des_zona_origen string COMMENT "Descripción de la Zona de Origen" ,
-- MAGIC   cod_zona_destino string COMMENT "Codigo de la Zona Destino" ,
-- MAGIC   des_zona_destino string COMMENT "Descripción de la Zona de Destino" ,
-- MAGIC   tabla_tarifas string,
-- MAGIC   tipo_tarifa string COMMENT "Codigo tipo de tarifa",
-- MAGIC   tipo_unidad string,
-- MAGIC   capacidad string,
-- MAGIC   grupo_porte_prod string,
-- MAGIC   tarifa string,
-- MAGIC   inicio_validez string,
-- MAGIC   fin_validez string,
-- MAGIC   CREATE_AT timestamp,
-- MAGIC   YEAR_MONTH_DAY string,
-- MAGIC   ORIGIN_FILE string
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_maestro_tarifas'

-- COMMAND ----------

select YEAR_MONTH_DAY, count(1)
from uf.fletes_maestro_tarifas
group by YEAR_MONTH_DAY

-- COMMAND ----------


