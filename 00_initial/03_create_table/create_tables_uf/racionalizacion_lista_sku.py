# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists uf.racionalizacion_lista_sku ;
# MAGIC create table uf.racionalizacion_lista_sku
# MAGIC (
# MAGIC sku string,
# MAGIC anio_racionalizacion string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_user_file/user-file/racionalizacion_lista_sku'
