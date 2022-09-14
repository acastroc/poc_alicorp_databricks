# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.pedido_cabecera ;
# MAGIC create table silver.pedido_cabecera
# MAGIC (
# MAGIC cod_pedido string,
# MAGIC fec_emision timestamp,
# MAGIC cod_tipo_documento string,
# MAGIC cod_clase_documento string,
# MAGIC mto_documento decimal(22,3),
# MAGIC cod_moneda_origen string,
# MAGIC fec_creacion timestamp,
# MAGIC fec_modificacion timestamp,
# MAGIC cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (cod_tipo_documento)
# MAGIC LOCATION '/mnt/data_entities/silver/pedido_cabecera'
