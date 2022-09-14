# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists uf.fletes_maestro_recursos ;
# MAGIC create table uf.fletes_maestro_recursos
# MAGIC (
# MAGIC   recurso string COMMENT "Codigo de los Recursos" ,
# MAGIC   medio_transporte string COMMENT "Tipo Medio de transporte" ,
# MAGIC   propietario string COMMENT "Codigo del ropietario del Recurso" ,
# MAGIC   placa string COMMENT "Placa del rescurso" ,
# MAGIC   clase_vehiculo string COMMENT "Codigo tipo de Documento" ,
# MAGIC   dimension_1 string  ,
# MAGIC   capacidad_1 string ,
# MAGIC   unidad_medida_1 string ,
# MAGIC   dimension_2 string ,
# MAGIC   capacidad_2 string ,
# MAGIC   unidad_medida_2 string ,
# MAGIC   dimension_3 string,
# MAGIC   capacidad_3 string,
# MAGIC   unidad_medida_3 string,
# MAGIC   dimension_4 string,
# MAGIC   capacidad_4 string,
# MAGIC   conductor_estandar string,
# MAGIC   tipo_agrupacion_recurso string,
# MAGIC   atributo_agrupacion_recurso string,
# MAGIC   CREATE_AT timestamp,
# MAGIC   YEAR_MONTH_DAY string,
# MAGIC   ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC 
# MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_maestro_recursos'

# COMMAND ----------



# COMMAND ----------


