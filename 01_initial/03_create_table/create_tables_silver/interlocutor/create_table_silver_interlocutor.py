# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.interlocutor ;
# MAGIC create table silver.interlocutor
# MAGIC (
# MAGIC cod_interlocutor string,
# MAGIC cod_pais string,
# MAGIC des_nombre_1 string,
# MAGIC des_nombre_2 string,
# MAGIC tip_documento_identidad string,
# MAGIC num_documento_identidad string,
# MAGIC cod_region string,
# MAGIC fec_creacion timestamp,
# MAGIC fec_modificacion timestamp,
# MAGIC cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (cod_pais,cod_region)
# MAGIC LOCATION '/mnt/data_entities/silver/interlocutor'
