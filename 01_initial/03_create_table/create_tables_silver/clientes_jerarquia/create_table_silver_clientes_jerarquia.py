# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.clientes_jerarquia ;
# MAGIC create table silver.clientes_jerarquia
# MAGIC ( 
# MAGIC     tip_jerarquia string COMMENT "Tipo de jerarquía",
# MAGIC 	cod_interlocutor string COMMENT "Código del interlocutor comercial de tipo cliente",
# MAGIC 	cod_grupo_interlocutor string COMMENT "Código del grupo de interlocutor (ZENT, ZDES, ZDEM, ZC, …)",
# MAGIC 	cod_organizacion_venta string COMMENT "cod_organizacion_venta",
# MAGIC 	cod_canal_distribucion string COMMENT "Código de canal de distribución",
# MAGIC 	cod_sector_comercial string COMMENT "Código de sector comercial",
# MAGIC 	num_nivel_jerarquia string COMMENT "Nivel de jerarquía del cliente",
# MAGIC 	cod_interlocutor_nodo_padre string COMMENT "Código del interlocutor comercial del nodo padre",
# MAGIC 	cod_grupo_interlocutor_nodo_padre string COMMENT "Código del grupo de interlocutor del nodo padre",
# MAGIC 	cod_organizacion_venta_nodo_padre string COMMENT "Código de organización de ventas del nodo padre",
# MAGIC 	cod_canal_distribucion_nodo_padre string COMMENT "Código de canal de distribución del nodo padre",
# MAGIC 	cod_sector_comercial_nodo_padre string COMMENT "Código de sector comercial del nodo padre",
# MAGIC 	fec_inicio_validez date COMMENT "Fecha de inicio de validez de la jerarquía",
# MAGIC 	fec_fin_validez date COMMENT "Fecha fin de validez de la jerarquía",
# MAGIC 	fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla"
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/clientes_jerarquia'
