# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.diario_unificado;
# MAGIC create table silver.diario_unificado
# MAGIC (
# MAGIC   cod_ledger string,
# MAGIC   cod_sociedad string,
# MAGIC   des_periodo_ejercicio string,
# MAGIC   num_documento_contable int,
# MAGIC   num_posicion_documento int,
# MAGIC   num_documento_referencia int,
# MAGIC   num_posicion_documento_referencia int,
# MAGIC   cod_moneda_origen string,
# MAGIC   num_cuenta string,
# MAGIC   cod_segmento_reporting string,
# MAGIC   mto_documento decimal(22,3),
# MAGIC   fec_contabilizacion_documento timestamp,
# MAGIC   cod_producto string,
# MAGIC   cod_centro string,
# MAGIC   cod_cliente string,
# MAGIC   num_contrato_condiciones string,
# MAGIC   cod_organizacion_ventas string,
# MAGIC   cod_canal_distribucion string,
# MAGIC   cod_sector string,
# MAGIC   cod_grupo_clientes string,
# MAGIC   cod_zona_ventas string,
# MAGIC   cod_destinatario_factura string,
# MAGIC   cod_grupo_condiciones_cliente_1 string,
# MAGIC   cod_pais string,
# MAGIC   cod_grupo_precio_cliente string,
# MAGIC   cod_grupo_material_1 string,
# MAGIC   cod_grupo_material_2 string,
# MAGIC   cod_plataforma string,
# MAGIC   cod_sub_plataforma string,
# MAGIC   cod_categoria string,
# MAGIC   cod_familia string,
# MAGIC   cod_variedad string,
# MAGIC   cod_presentacion string,
# MAGIC   cod_tipo_lista_precios string,
# MAGIC   cod_oficina_ventas string,
# MAGIC   cod_grupo_vendedores string,
# MAGIC   fec_creacion timestamp,
# MAGIC   fec_modificacion timestamp,
# MAGIC   cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (des_periodo_ejercicio)
# MAGIC LOCATION '/mnt/data_entities/silver/diario_unificado'
