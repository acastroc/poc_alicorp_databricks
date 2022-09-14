# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.lista_materiales_componente ;
# MAGIC create table silver.lista_materiales_componente
# MAGIC (
# MAGIC cod_lista_materiales String COMMENT "Código de lista de materiales ",
# MAGIC cod_alternativa_lmt String COMMENT "Código de alternativa de la lista de materiales ",
# MAGIC cod_tipo_lmt String COMMENT "Código de tipo de lista de materiales (M) ",
# MAGIC cod_componente int COMMENT "Código interno del componente ",
# MAGIC cod_posicion_componente String COMMENT "Código de la posición del componenten en la interfaz de SAP ",
# MAGIC cod_documento_modificacion String COMMENT "Código del documento de modificación usado para el componente ",
# MAGIC cod_material_componente String COMMENT "Código del material componente ",
# MAGIC cod_centro_origen String COMMENT "Código del centro origen del material ",
# MAGIC num_cantidad_material_componente decimal(22,3) COMMENT "Cantidad del material componente para la lista ",
# MAGIC cod_unidad_material_componente String COMMENT "Unidad de medida del material componente ",
# MAGIC fec_creacion String COMMENT "Fecha de creación del componente de la lista de materiales ",
# MAGIC fec_ultima_modificacion String COMMENT "Fecha de última modificación del componente de la lista de materiales ",
# MAGIC fec_fin_validez date COMMENT "Fecha fin de validez del componente de la lista de materiales ",
# MAGIC fecha_carga Timestamp COMMENT "Fecha y hora de carga de la tabla "
# MAGIC 
# MAGIC )
# MAGIC --using delta
# MAGIC --LOCATION '/mnt/data_entities/silver/lista_materiales_componente'

# COMMAND ----------


