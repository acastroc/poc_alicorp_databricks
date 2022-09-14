# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.agencia_cabecera;
# MAGIC create table silver.agencia_cabecera
# MAGIC (
# MAGIC num_documento int,
# MAGIC cod_clase_documento_liquidacion string,
# MAGIC num_documento_referencia int,
# MAGIC cod_clase_pedido_abierto_condicion string,
# MAGIC cod_territorio string,
# MAGIC fec_creacion timestamp,
# MAGIC fec_modificacion timestamp,
# MAGIC cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (cod_sistema_origen)
# MAGIC LOCATION '/mnt/data_entities/silver/agencia_cabecera'
