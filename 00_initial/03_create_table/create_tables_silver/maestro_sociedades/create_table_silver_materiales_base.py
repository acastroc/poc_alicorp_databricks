# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.materiales_base ;
# MAGIC create table silver.materiales_base
# MAGIC (
# MAGIC COD_MATERIAL string COMMENT "Código del material en S4",
# MAGIC DES_MATERIAL string COMMENT "Descripción del material ",
# MAGIC COD_TIPO_MATERIAL string COMMENT "Código del tipo de material en nomenclatura S4 ",
# MAGIC DES_TIPO_MATERIAL string COMMENT "Descripción del tipo de material ",
# MAGIC COD_JERARQUIA string COMMENT "Código de la jerarquía del material (18 dígitos) ",
# MAGIC COD_GRUPO_TRANSPORTE string COMMENT "Código de grupo de transporte ",
# MAGIC DES_GRUPO_TRANSPORTE string COMMENT "Descripción de grupo de transporte ",
# MAGIC COD_DUENIO_MARCA string COMMENT "Código de dueño de marca / fabricante ",
# MAGIC DES_DUENIO_MARCA string COMMENT "Descripción de dueño de marca / fabricante ",
# MAGIC COD_ONU_CUBSO string COMMENT "Código internacional para el material (Cubso) ",
# MAGIC COD_UNIDAD_BASE string COMMENT "Código de la unidad de medida base del material ",
# MAGIC NUM_PESO_NETO_UNIDAD_BASE decimal(22,3) COMMENT "Peso neto del material en su unidad base ",
# MAGIC NUM_PESO_BRUTO_UNIDAD_BASE decimal(22,3) COMMENT "Peso bruto del material en su unidad base ",
# MAGIC COD_UNIDAD_PESO_BASE string COMMENT "Código de la unidad de peso del material en su unidad base ",
# MAGIC NUM_VOLUMEN_UNIDAD_BASE decimal(22,3) COMMENT "Volumen del material en su unidad base ",
# MAGIC COD_UNIDAD_VOLUMEN_BASE string COMMENT "Código de la unidad de volumen del material en su unidad base ",
# MAGIC COD_EAN13 string COMMENT "Código EAN13 (código de barras) ",
# MAGIC NUM_ALTO_UNIDAD_BASE decimal(22,3) COMMENT "Altura del material en su unidad base ",
# MAGIC NUM_ANCHO_UNIDAD_BASE decimal(22,3) COMMENT "Ancho del material en su unidad base ",
# MAGIC NUM_PROFUNDIDAD_UNIDAD_BASE int COMMENT "Profundidad del material en su unidad base ",
# MAGIC COD_UNIDAD_DIMENSIONES_BASE string COMMENT "Código de la unidad de las dimensiones del material en su unidad base ",
# MAGIC NUM_TIEMPO_DURACION decimal(22,3) COMMENT "Tiempo de duración ",
# MAGIC NUM_DURACION_TOTAL int COMMENT "Tiempo total del material ",
# MAGIC COD_UNIDAD_TIEMPO string COMMENT "Código de la unidad de medida del tiempo ",
# MAGIC DES_UNIDAD_TIEMPO string COMMENT "Descripción de la unidad de medida del tiempo ",
# MAGIC COD_UNIDAD_ALMACENAMIENTO string COMMENT "Código de la unidad de medida de almacenamiento del material ",
# MAGIC NUM_NUMERADOR_CONVERSION_UNIDAD_ALMACENAMIENTO int COMMENT "Numerador para la conversión de unidad base a unidad de almacenamiento ",
# MAGIC NUM_DENOMINADOR_CONVERSION_UNIDAD_ALMACENAMIENTO int COMMENT "Denominador para la conversión de unidad base a unidad de almacenamiento ",
# MAGIC COD_EAN14 string COMMENT "Código EAN14 (código de barras) ",
# MAGIC NUM_PESO_BRUTO_ALMACENAMIENTO decimal(22,3) COMMENT "Peso bruto del material en su unidad de almacenamiento ",
# MAGIC COD_UNIDAD_PESO_ALMACENAMIENTO string COMMENT "Código de la unidad de peso del material en su unidad de almacenamiento ",
# MAGIC NUM_ALTO_UNIDAD_ALMACENAMIENTO decimal(22,3) COMMENT "Altura del material en su unidad de almacenamiento ",
# MAGIC NUM_ANCHO_UNIDAD_ALMACENAMIENTO decimal(22,3) COMMENT "Ancho del material en su unidad de almacenamiento ",
# MAGIC NUM_PROFUNDIDAD_UNIDAD_ALMACENAMIENTO decimal(22,3) COMMENT "Profundidad del material en su unidad de almacenamiento ",
# MAGIC COD_UNIDAD_DIMENSIONES_ALMACENAMIENTO string COMMENT "Código de la unidad de las dimensiones del material en su unidad de almacenamiento ",
# MAGIC NUM_VOLUMEN_UNIDAD_ALMACENAMIENTO decimal(22,3) COMMENT "Volumen del material en su unidad de almacenamiento ",
# MAGIC COD_UNIDAD_VOLUMEN_ALMACENAMIENTO string COMMENT "Código de la unidad de volumen del material en su unidad de almacenamiento ",
# MAGIC FLAG_MATERIAL_ALICORP integer COMMENT "Flag: Si el material está extendido en una Org de ventas de Alicorp Perú (1); caso contrario (0) ",
# MAGIC COD_UNIDAD_COMERCIAL string COMMENT "Código de la unidad de medida comercial del material (UCO) ",
# MAGIC NUM_NUMERADOR_CONVERSION_UNIDAD_COMERCIAL int COMMENT "Numerador para la conversión de unidad base a unidad comercial ",
# MAGIC NUM_DENOMINADOR_CONVERSION_UNIDAD_COMERCIAL int COMMENT "Denominador para la conversión de unidad base a unidad comercial ",
# MAGIC COD_BLOQUEO STRING COMMENT "Código de bloqueo general del material",
# MAGIC DES_BLOQUEO STRING COMMENT "Descripción del bloqueo general del material",
# MAGIC FECHA_CARGA timestamp COMMENT "Fecha y hora de carga de la tabla "
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC LOCATION '/mnt/data_entities/silver/materiales_base'
