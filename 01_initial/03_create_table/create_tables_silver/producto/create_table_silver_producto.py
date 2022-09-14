# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.producto ;
# MAGIC create table silver.producto
# MAGIC (
# MAGIC cod_producto string,
# MAGIC des_producto string,
# MAGIC cod_tipo_producto string,
# MAGIC cod_ramo string,
# MAGIC cod_grupo_producto string,
# MAGIC cod_unidad_medida_base string,
# MAGIC cod_unidad_medida_pedido string,
# MAGIC num_peso_bruto decimal(22,3),
# MAGIC num_peso_neto decimal(22,3),
# MAGIC cod_unidad_peso string,
# MAGIC num_volumen decimal(22,3),
# MAGIC cod_unidad_volumen string,
# MAGIC cod_jerarquia_producto string,
# MAGIC cod_categoria_producto string,
# MAGIC fec_creacion timestamp,
# MAGIC fec_modificacion timestamp,
# MAGIC cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (cod_tipo_producto)
# MAGIC LOCATION '/mnt/data_entities/silver/producto'
