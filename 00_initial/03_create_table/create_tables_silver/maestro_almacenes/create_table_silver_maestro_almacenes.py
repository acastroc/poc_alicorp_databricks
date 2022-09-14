# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.maestro_almacenes ;
# MAGIC create table silver.maestro_almacenes
# MAGIC (
# MAGIC cod_almacen string COMMENT "Código del almacén",
# MAGIC des_almacen string COMMENT "Nombre del almacén",
# MAGIC cod_centro string COMMENT "Código del centro donde se encuentra ubicado el almacén",
# MAGIC cod_sociedad string COMMENT "Código de la sociedad a la que pertenece el centro",
# MAGIC cod_pais string COMMENT "Codigo del país del centro",
# MAGIC fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla "
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/maestro_almacenes'
