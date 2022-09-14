# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.venta_producto ;
# MAGIC create table silver.venta_producto
# MAGIC (
# MAGIC COD_PRODUCTO	STRING
# MAGIC ,COD_ORGANIZACION_VENTAS	STRING
# MAGIC ,COD_CANAL_DISTRIBUCION	STRING
# MAGIC ,COD_JERARQUIA_PRODUCTO	STRING
# MAGIC ,COD_UNIDAD_VENTA	STRING
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION '/mnt/data_entities/silver/venta_producto'
