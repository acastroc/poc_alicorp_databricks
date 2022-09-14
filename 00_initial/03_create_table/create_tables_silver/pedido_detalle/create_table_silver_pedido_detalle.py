# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.pedido_detalle;
# MAGIC create table silver.pedido_detalle
# MAGIC (
# MAGIC   cod_pedido string,
# MAGIC   num_posicion_pedido int,
# MAGIC   cod_producto string,
# MAGIC   num_lote int,
# MAGIC   cod_grupo_producto string,
# MAGIC   cod_tipo_posicion string,
# MAGIC   cod_jerarquia_producto string,
# MAGIC   mto_previsto decimal(22,3),
# MAGIC   ctd_prevista decimal(22,3),
# MAGIC   cod_unidad_medida_base_prevista string,
# MAGIC   cod_unidad_medida_base string,
# MAGIC   mto_neto_detalle decimal(22,3),
# MAGIC   fec_creacion timestamp,
# MAGIC   fec_modificacion timestamp,
# MAGIC   cod_sistema_origen string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (cod_grupo_producto)
# MAGIC LOCATION '/mnt/data_entities/silver/pedido_detalle'
