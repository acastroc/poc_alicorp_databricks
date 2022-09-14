# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.materiales_jerarquia ;
# MAGIC create table silver.materiales_jerarquia
# MAGIC (
# MAGIC COD_JERARQUIA string COMMENT "Código de 18 dígitos de la jerarquía del material. Aplica para materiales ZFER, ZHAW, ZFER, ZROH " ,
# MAGIC COD_PLATAFORMA string COMMENT "Código del primer nivel de jerarquía: Plataforma. Se encuentra en los dos primeros dígitos del Códig de jerarquía" ,
# MAGIC DES_PLATAFORMA string COMMENT "Descripción del primer nivel de jerarquía del material: Plataforma" ,
# MAGIC COD_SUBPLATAFORMA string COMMENT "Código del segundo nivel de jerarquía: Subplataforma. Se encuentra en los dígitos 3 y 4 del Código de jerarquía" ,
# MAGIC DES_SUBPLATAFORMA string COMMENT "Descripción del segundo nivel de jerarquía del material: Subplataforma" ,
# MAGIC COD_CATEGORIA string COMMENT "Código del tercer nivel de jerarquía: Categoría. Se encuentra en los dígitos 5, 6 y 7 del Código de jerarquía" ,
# MAGIC DES_CATEGORIA string COMMENT "Descripción del tercer nivel de jerarquía del material: Categoría" ,
# MAGIC COD_FAMILIA string COMMENT "Código del cuarto nivel de jerarquía: Familia. Se encuentra en los dígitos 8, 9 y 10 del Código de jerarquía" ,
# MAGIC DES_FAMILIA string COMMENT "Descripción del cuarto nivel de jerarquía del material: Familia" ,
# MAGIC COD_VARIEDAD string COMMENT "Código del quinto nivel de jerarquía: Variedad. Se encuentra en los dígitos 11, 12 y 13 del Código de jerarquía" ,
# MAGIC DES_VARIEDAD string COMMENT "Descripción del quinto nivel de jerarquía del material: Variedad" ,
# MAGIC COD_PRESENTACION string COMMENT "Código del sexto nivel de jerarquía: Presentación. Se encuentra en los 5 último dígitos del Código de jerarquía" ,
# MAGIC DES_PRESENTACION string COMMENT "Descripción del sexto nivel de jerarquía del material: Presentación" ,
# MAGIC FECHA_CARGA timestamp COMMENT "Fecha y hora de carga de la tabla"
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/materiales_jerarquia'
