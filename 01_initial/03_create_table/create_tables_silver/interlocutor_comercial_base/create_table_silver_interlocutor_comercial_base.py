# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.interlocutor_comercial_base ;
# MAGIC create table silver.interlocutor_comercial_base
# MAGIC (
# MAGIC     cod_interlocutor string   COMMENT "Código del interlocutor comercial",
# MAGIC 	flg_empresa string   COMMENT "Flag: Si el interlocutor tiene el campo TYPE=2 (X); caso contrario está vacío. Nos indica si el interlocutor es de tipo Empresa en SAP",
# MAGIC 	flg_persona_natural string   COMMENT "Flag: Si el interlocutor tiene el campo NATPERS lleno (X); caso contrario está vacío. Nos indica si el interlocutor tiene la marca de Persona Natural",
# MAGIC 	tip_interlocutor_especial string   COMMENT "El campo indica si el interlocutor es de algún tipo especial: Accionista, Relacionado, Transportista",
# MAGIC 	cod_grupo_interlocutor string   COMMENT "Código del grupo de interlocutor (ZENT, ZDES, ZDEM, ZC, …)",
# MAGIC 	des_grupo_interlocutor string   COMMENT "Descripción del grupo de interlocutor",
# MAGIC 	des_nombre_interlocutor string   COMMENT "Nombre el interlocutor comercial (razón social en interlocutores de tipo empresa)",
# MAGIC 	flg_cliente string   COMMENT "Flag: Si el interlocutor es de tipo Cliente (X); caso contraro está vacío",
# MAGIC 	flg_bloqueo_cliente string   COMMENT "Flag: Si el interlocutor tiene un bloqueo general como Cliente (X); caso contrario está vacío",
# MAGIC 	flg_proveedor string   COMMENT "Flag: Si el interlocutor es de tipo Proveedor (X); caso contraro está vacío",
# MAGIC 	flg_bloqueo_proveedor string   COMMENT "Flag: Si el interlocutor tiene un bloqueo general como Proveedor (X); caso contrario está vacío",
# MAGIC 	cod_pais string   COMMENT "Código del país del interlocutor comercial ",
# MAGIC 	cod_idioma string   COMMENT "Código del idioma del interlocutor ",
# MAGIC 	cod_region string   COMMENT "Código de región de la dirección del interlocutor ",
# MAGIC 	des_poblacion string   COMMENT "Nombre de la poblacion de la dirección del interlocutor ",
# MAGIC 	des_distrito string   COMMENT "Nombre del distrito de la direccion del interlocutor ",
# MAGIC 	des_direccion string   COMMENT "Dirección/Domicilio fiscal del interlocutor ",
# MAGIC 	cod_zona_transporte string   COMMENT "Descripción del grupo de compras del material ",
# MAGIC 	cod_contacto string   COMMENT "Código del número de contacto interno de SAP ",
# MAGIC 	fec_creacion date   COMMENT "Fecha de creación del interlocutor ",
# MAGIC 	cod_usuario_creador string   COMMENT "Código de usuario creador del interlocutor",
# MAGIC 	fec_ultima_modificacion date   COMMENT "Fecha de última modificación del interlocutor ",
# MAGIC 	cod_usuario_ultima_modif string   COMMENT "Código de usuario de última modificación ",
# MAGIC     fecha_carga timestamp COMMENT "Fecha y hora de carga de la tabla"
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION '/mnt/data_entities/silver/interlocutor_comercial_base'
