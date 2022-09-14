# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.facturacion_cabecera ;
# MAGIC create table silver.facturacion_cabecera
# MAGIC (
# MAGIC cod_factura	string,
# MAGIC cod_clase_factura	string,
# MAGIC cod_tipo_documento_comercial	string,
# MAGIC cod_moneda_origen	string,
# MAGIC cod_organizacion_ventas	string,
# MAGIC cod_canal_distribucion	string,
# MAGIC cod_condicion_expedicion	string,
# MAGIC fec_emision_factura	date,
# MAGIC des_periodo_ejercicio	string,
# MAGIC cod_grupo_precio_cliente	string,
# MAGIC cod_grupo_cliente	string,
# MAGIC cod_zona_ventas	string,
# MAGIC cod_tipo_lista_precios	string,
# MAGIC cod_condicion_pago	string,
# MAGIC cod_pais_destino	string,
# MAGIC cod_sociedad	string,
# MAGIC mto_neto_factura	decimal(22,3),
# MAGIC cod_responsable_pago	string,
# MAGIC cod_solicitante	string,
# MAGIC num_documento_anulado	string,
# MAGIC cod_sector	string,
# MAGIC fec_creacion	date,
# MAGIC fec_modificacion	date,
# MAGIC cod_sistema_origen	string,
# MAGIC year_month string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/facturacion_cabecera'
