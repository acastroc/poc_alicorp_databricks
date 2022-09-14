# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.materiales_almacen ;
# MAGIC create table silver.materiales_almacen
# MAGIC ( 
# MAGIC cod_material string COMMENT "Código del material en S4",
# MAGIC cod_tipo_material string COMMENT "Código del tipo de material en nomenclatura S4",
# MAGIC cod_centro string COMMENT "Código del centro de prod/alm/distr",
# MAGIC cod_almacen string COMMENT "Código de almacén dentro del centro",
# MAGIC cod_estado_mantenimiento string COMMENT "Código de estado de mantenimiento",
# MAGIC fec_anio_fiscal int  COMMENT "Año fiscal del registro",
# MAGIC fec_mes_carga int COMMENT "Mes de carga del registro ",
# MAGIC cant_stock_libre_utilizacion decimal(23,3) COMMENT "Stock de libre utilización",
# MAGIC cant_stock_lotes_restringidos decimal(23,3) COMMENT "Stock de lotes restringidos",
# MAGIC cant_stock_en_traslado decimal(23,3) COMMENT "Stock en traslado",
# MAGIC cant_stock_bloqueado decimal(23,3) COMMENT "Stock bloqueado",
# MAGIC cant_stock_en_inspeccion_calidad decimal(23,3) COMMENT "Stock en inspeccion de calidad",
# MAGIC fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla "
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/materiales_almacen'
