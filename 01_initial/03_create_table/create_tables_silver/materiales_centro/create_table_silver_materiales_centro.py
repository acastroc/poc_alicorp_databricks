# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.materiales_centro ;
# MAGIC create table silver.materiales_centro
# MAGIC ( 
# MAGIC cod_material String COMMENT "Código del material en S4 ",
# MAGIC cod_tipo_material String COMMENT "Código del tipo de material en nomenclatura S4 ",
# MAGIC cod_jerarquia String COMMENT "Código de la jerarquía del material (18 dígitos) ",
# MAGIC cod_centro String COMMENT "Código de centro de almacenamiento/distribución ",
# MAGIC cod_sociedad String COMMENT "Código de sociedad ",
# MAGIC cod_pais String COMMENT "Código de país ",
# MAGIC tip_material_fabricacion String COMMENT "Tipo de material de fabricación calculado según parámetros de aprovisionamiento ",
# MAGIC cod_bloqueo_centro String COMMENT "Código de bloqueo del material a nivel centro ",
# MAGIC des_bloqueo_centro String COMMENT "Descripción de bloqueo del material a nivel centro ",
# MAGIC cod_unidad_base String COMMENT "Código de la unidad de medida base del material ",
# MAGIC cod_unidad_almacenamiento String COMMENT "Código de la unidad de medida de almacenamiento del material ",
# MAGIC cod_caracteristica_planificacion String COMMENT "Código de la característica de planificación ",
# MAGIC des_caracteristica_planificacion String COMMENT "Descripción de la característica de planificación ",
# MAGIC cod_clase_aprovisionamiento String COMMENT "Código de la clase de aprovisionamiento del material ",
# MAGIC cod_planificacion_necesidades String COMMENT "Código de planificación de necesidades ",
# MAGIC des_planificacion_necesidades String COMMENT "Descripción de planificación de necesidades ",
# MAGIC cod_grupo_compras String COMMENT "Código del grupo de compras del material ",
# MAGIC des_grupo_compras String COMMENT "Descripción del grupo de compras del material ",
# MAGIC cod_almacen_aprovisionamiento_externo String COMMENT "Código del almacén de aprovisionamiento externo ",
# MAGIC cod_almacen_produccion String COMMENT "Código del almacén de producción ",
# MAGIC cod_grupo_impo_expo String COMMENT "Código del grupo impo/expo ",
# MAGIC cod_aprovisionamiento_especial String COMMENT "Código de aprovisionamiento especial ",
# MAGIC des_aprovisionamiento_especial String COMMENT "Descripción de aprovisionamiento especial ",
# MAGIC cod_centro_origen String COMMENT "Código del centro origen del material ",
# MAGIC cod_sociedad_origen String COMMENT "Código de la sociedad origen del material ",
# MAGIC cod_grupo_carga String COMMENT "Código del grupo de carga ",
# MAGIC des_grupo_carga String COMMENT "Descripción del grupo de carga ",
# MAGIC cod_grupo_tratamiento_logistico String COMMENT "Código de grupo de tratamiento logístico ",
# MAGIC cod_grupo_planificacion String COMMENT "Código de grupo de planificación ",
# MAGIC cod_centro_beneficio String COMMENT "Descripción de grupo de planificación ",
# MAGIC cod_estado_mantenimiento String COMMENT "Código de estado de mantenimiento ",
# MAGIC flg_vista_compras int COMMENT "Flag: Si el material tiene habilitada la vista de compras (1); caso contrario (0) ",
# MAGIC flg_vista_ventas int COMMENT "Flag: Si el material tiene habilitada la vista de ventas (1); caso contrario (0) ",
# MAGIC num_tiempo_entrega_previsto int COMMENT "Tiempo de entrega previsto ",
# MAGIC num_tiempo_trat_entrada_mercancia int COMMENT "Tiempo de tratamiento para entrada de mercancía ",
# MAGIC cod_categoria_valoracion String COMMENT "Código de categoría de valoración ",
# MAGIC des_categoria_valoracion String COMMENT "Descripción de categoría de valoración ",
# MAGIC cod_indicador_control_precio String COMMENT "Código de indicador de control de precio ",
# MAGIC num_precio_valorizado Decimal(16,2) COMMENT "Precio valorizado actual ",
# MAGIC num_cantidad_base int COMMENT "Cantidad base valorizada actual ",
# MAGIC cod_indicador_impuestos String COMMENT "Código del indicador de impuestos ",
# MAGIC des_indicador_impuestos String COMMENT "Descripción del indicador de impuestos ",
# MAGIC fecha_carga Timestamp COMMENT "Fecha y hora de carga de la tabla "
# MAGIC 
# MAGIC 
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC --LOCATION '/mnt/data_entities/silver/materiales_centro'

# COMMAND ----------


