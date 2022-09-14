# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.jerarquia_producto ;
# MAGIC create table silver.jerarquia_producto
# MAGIC (
# MAGIC cod_jerarquia_producto string,
# MAGIC cod_plataforma string,
# MAGIC des_plataforma string,
# MAGIC cod_sub_plataforma string,
# MAGIC des_sub_plataforma string,
# MAGIC cod_categoria string,
# MAGIC des_categoria string,
# MAGIC cod_familia string,
# MAGIC des_familia string,
# MAGIC cod_variedad string,
# MAGIC des_variedad string,
# MAGIC cod_presentacion string,
# MAGIC des_presentacion string,
# MAGIC fec_creacion timestamp,
# MAGIC cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (cod_plataforma)
# MAGIC LOCATION '/mnt/data_entities/silver/jerarquia_producto'
