# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.clientes_organizacion_venta;
# MAGIC create table silver.clientes_organizacion_venta
# MAGIC (
# MAGIC     cod_interlocutor  string COMMENT "Código del interlocutor comercial de tipo cliente",
# MAGIC 	cod_grupo_interlocutor string COMMENT "Código del grupo de interlocutor (ZENT, ZDES, ZDEM, ZC, …)",
# MAGIC 	cod_organizacion_venta string COMMENT "Código de organización de ventas",
# MAGIC 	cod_canal_distribucion string COMMENT "Código de canal de distribución",
# MAGIC 	cod_sector_comercial string COMMENT "Código de sector comercial",
# MAGIC 	cod_oficina_ventas string COMMENT "Código de la oficina de ventas (Canal comercial)",
# MAGIC 	des_oficina_ventas string COMMENT "Descripción de la oficina de ventas (Tradicional, Moderno, CMI, etc)",
# MAGIC 	cod_zona_clientes string COMMENT "Código de zona de clientes",
# MAGIC 	des_zona_clientes string COMMENT "Descripción de zona de cliente",
# MAGIC 	cod_grupo_vendedores string COMMENT "Código del grupo de vendedores asociado al cliente",
# MAGIC 	des_grupo_venderores string COMMENT "Descripción del grupo de vendedores asociado al cliente",
# MAGIC 	cod_grupo_clientes string COMMENT "Código del grupo de clientes",
# MAGIC 	des_grupo_clientes string COMMENT "Descripción del grupo de clientes",
# MAGIC 	cod_grupo_precios string COMMENT "Código del grupo de precios",
# MAGIC 	des_grupo_precios string COMMENT "Descripción del grupo precios",
# MAGIC 	cod_lista_precios string COMMENT "Código de la lista de precios",
# MAGIC 	des_lista_precios string COMMENT "Descripción de la lista precios",
# MAGIC 	cod_segmento_cliente string COMMENT "Segmento del cliente",
# MAGIC 	cod_grupo_imputacion string COMMENT "Código del grupo de imputación",
# MAGIC 	des_grupo_imputacion string COMMENT "Descripción del grupo de imputación",
# MAGIC 	cod_condicion_pago string COMMENT "Código de la condición de pago del cliente",
# MAGIC 	cod_condicion_expedicion string COMMENT "Código de la condición de expedición del cliente",
# MAGIC 	des_condicion_expedicion string COMMENT "Descripción de la condición de expedición del cliente",
# MAGIC 	cod_moneda string COMMENT "Moneda del cliente",
# MAGIC 	num_prioridad_entrega INT  COMMENT "Prioridad de entrega",
# MAGIC 	fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla"
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION '/mnt/data_entities/silver/clientes_organizacion_venta'
