# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.maestro_sociedades ;
# MAGIC create table silver.maestro_sociedades
# MAGIC (
# MAGIC cod_sociedad string COMMENT "Código de la sociedad del grupo Alicorp",
# MAGIC des_sociedad string COMMENT "Descripción de la sociedad",
# MAGIC cod_pais string COMMENT "Código del país de la sociedad", 
# MAGIC des_pais string COMMENT "Descripción del país de la sociedad", 
# MAGIC cod_moneda string COMMENT "Código de moneda de la sociedad", 
# MAGIC cod_posicion_presupuestaria string COMMENT "Código de la posición presupuestaria de la sociedad", 
# MAGIC fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla "
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/maestro_sociedades'
