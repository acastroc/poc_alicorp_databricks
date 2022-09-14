# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.lista_materiales_cabecera ;
# MAGIC create table silver.lista_materiales_cabecera
# MAGIC ( 
# MAGIC     cod_material string COMMENT "Código del material",
# MAGIC 	cod_tipo_material  string COMMENT "Código del tipo de material en nomenclatura S4",
# MAGIC 	cod_centro  string COMMENT "Código del centro prod/alm/distr",
# MAGIC 	cod_jerarquia  string COMMENT "Código de jerarquía del material",
# MAGIC 	tip_material_fabricacion  string COMMENT "Tipo de material de fabricación calculado según parámetros de aprovisionamiento",
# MAGIC 	cod_lista_materiales string COMMENT "Código de lista de materiales",
# MAGIC     cod_alternativa_lmt  string COMMENT "Código de alternativa de la lista de materiales",
# MAGIC     cod_tipo_lmt  string COMMENT "Código de tipo de lista de materiales (M)",
# MAGIC     cod_utilizacion_lmt  string COMMENT "Código de utilización de la lista de materiales (1 o 6)",
# MAGIC     num_cantidad_produccion  decimal(23,3) COMMENT "Cantidad de material producido con la lista de materiales",
# MAGIC     cod_unidad_medida_produccion  string COMMENT "Unidad de medida de producción del material",
# MAGIC     cod_documento_modificacion  string COMMENT "Código del documento de modificación con el que se configuró la lista de materiales",
# MAGIC     fec_creacion  date COMMENT "Fecha de creación de la lista de materiales",
# MAGIC     fec_ultima_modificacion  date COMMENT "Fecha de última modificación de la lista de materiales",
# MAGIC     fec_inicio_validez  date COMMENT "Fecha de inicio de validez de la lista de materiales",
# MAGIC     fec_fin_validez  date COMMENT "Fecha fin de validez de la lista de materiales",
# MAGIC     flg_version_fabricacion  int COMMENT "Flag: Si la lista de materiales tiene versión de fabricación (1). Caso contrario (0)",
# MAGIC     fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla"
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/lista_materiales_cabecera'
