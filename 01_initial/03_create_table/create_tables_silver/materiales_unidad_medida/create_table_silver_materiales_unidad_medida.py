# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.materiales_unidad_medida ;
# MAGIC create table silver.materiales_unidad_medida
# MAGIC ( 
# MAGIC 
# MAGIC cod_material string COMMENT "Código del material en S4 ",
# MAGIC cod_tipo_material string COMMENT "Código del tipo de material en nomenclatura S4 ",
# MAGIC cod_unidad_medida string COMMENT "Código de la unidad de medida del material ",
# MAGIC flg_unidad_base string COMMENT "Flag: Si la unidad de medida es igual a la unidad base (X) ",
# MAGIC num_numerador_conversion int COMMENT "Numerador para la conversión de unidad base a la unidad de medida del registro ",
# MAGIC num_denominador_conversion int COMMENT "Denominador para la conversión de unidad base a la unidad de medida del registro ",
# MAGIC cod_barras_ean11 string COMMENT "Código de barras EAN11 ",
# MAGIC num_peso_bruto decimal(23,3) COMMENT "Peso bruto del material en la unidad de medida del registro ",
# MAGIC cod_unidad_peso_bruto string COMMENT "Código de la unidad de peso del material ",
# MAGIC num_alto_material decimal(23,3) COMMENT "Alto del material en la unidad de medida del registro ",
# MAGIC num_ancho_material decimal(23,3) COMMENT "Ancho del material en la unidad de medida del registro ",
# MAGIC num_profundidad_material decimal(23,3) COMMENT "Profundidad del material en la unidad de medida del registro ",
# MAGIC cod_unidad_dimensiones string COMMENT "Código de la unidad de dimensiones (alto, largo, ancho) del material ",
# MAGIC num_volumen_material decimal(23,3) COMMENT "Volumen del material en la unidad de medida del registro ",
# MAGIC cod_unidad_volumen string COMMENT "Código de la unidad de volumen del material ",
# MAGIC fecha_carga timestamp COMMENT "fecha_carga "
# MAGIC     
# MAGIC 
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/materiales_unidad_medida'
