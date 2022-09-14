-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists datagov.maestro_tarifas_raw;
-- MAGIC create table datagov.maestro_tarifas_raw
-- MAGIC (
-- MAGIC   cod_proveedor string COMMENT "Codigo del Proveedor" ,
-- MAGIC   razon_social string COMMENT "Razon social del Proveedor" ,
-- MAGIC   cod_zona_expedicion string COMMENT "Codigo de la Zona de Expedicion" ,
-- MAGIC   des_zona_expedicion string COMMENT "Nombre de la Zona de Expedicion",
-- MAGIC   cod_zona_destino string COMMENT "Codigo de la Zona Destino" ,
-- MAGIC   des_zona_destino string COMMENT "Nombre de la Zona de Destino" ,
-- MAGIC   tabla_tarifas string COMMENT "Tabla origen de la tarifa",
-- MAGIC   tipo_tarifa string COMMENT "Codigo tipo de tarifa",
-- MAGIC   tipo_unidad string COMMENT "Paletizado / Estibado",
-- MAGIC   capacidad decimal(15,4) COMMENT "Capacidad del vehículo de transporte",
-- MAGIC   grupo_porte_prod string COMMENT "Condiciones de almacenaje",
-- MAGIC   tarifa decimal(15,4) COMMENT "Precio de la tarifa de transporte",
-- MAGIC   inicio_validez date,
-- MAGIC   fin_validez date
-- MAGIC )
-- MAGIC using delta
-- MAGIC ----partitioned by (YEAR_MONTH_DAY)
-- MAGIC 
-- MAGIC LOCATION '/mnt/data_governance/fletes/maestro_tarifas_raw'

-- COMMAND ----------



-- COMMAND ----------


