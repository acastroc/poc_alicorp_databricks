# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists gold.lista_materiales_exclusivos ;
# MAGIC create table gold.lista_materiales_exclusivos
# MAGIC ( 
# MAGIC cod_centro string COMMENT "Código del material en S4 " ,
# MAGIC cod_material	string COMMENT "Código del material",
# MAGIC des_material string COMMENT "Descripción del material",
# MAGIC cod_jerarquia  string COMMENT "Código de jerarquía del material",
# MAGIC cod_plataforma	string COMMENT "Código del primer nivel de jerarquía: Plataforma. Se encuentra en los dos primeros dígitos del Códig de jerarquía",
# MAGIC des_plataforma string COMMENT "Descripción del primer nivel de jerarquía del material: Plataforma",
# MAGIC cod_subplataforma string COMMENT "Código del segundo nivel de jerarquía: Subplataforma. Se encuentra en los dígitos 3 y 4 del Código de jerarquía",
# MAGIC des_subplataforma string COMMENT "Descripción del segundo nivel de jerarquía del material: Subplataforma",
# MAGIC cod_categoria string COMMENT "Código del tercer nivel de jerarquía: Categoría. Se encuentra en los dígitos 5, 6 y 7 del Código de jerarquía",
# MAGIC des_categoria string COMMENT "Descripción del tercer nivel de jerarquía del material: Categoría",
# MAGIC cod_familia	string COMMENT "Código del cuarto nivel de jerarquía: Familia. Se encuentra en los dígitos 8, 9 y 10 del Código de jerarquía",
# MAGIC des_familia	string COMMENT "Descripción del cuarto nivel de jerarquía del material: Familia",
# MAGIC cod_variedad string COMMENT "Código del quinto nivel de jerarquía: Variedad. Se encuentra en los dígitos 11, 12 y 13 del Código de jerarquía",
# MAGIC des_variedad string COMMENT "Descripción del quinto nivel de jerarquía del material: Variedad",
# MAGIC cod_presentacion string COMMENT "Código del sexto nivel de jerarquía: Presentación. Se encuentra en los 5 último dígitos del Código de jerarquía",
# MAGIC des_presentacion string COMMENT "Descripción del sexto nivel de jerarquía del material: Presentación", 
# MAGIC cod_material_componente string COMMENT "Código del material componente",
# MAGIC des_componente string COMMENT "Descripción del material",
# MAGIC cod_unidad_material_componente string COMMENT  "Unidad de medida del material componente",
# MAGIC num_cantidad_material_componente decimal(22,3) COMMENT "Cantidad del material componente para la lista", 
# MAGIC cod_centro_origen string COMMENT "Código del centro origen del material",
# MAGIC cod_tipo_material string COMMENT "Código del tipo de material en nomenclatura S4",
# MAGIC flag_racionalizacion_sku int COMMENT "1 si el SKU se va racionalizar",
# MAGIC flag_componente_exclusivo int COMMENT "1 si el sku se va racionalizar y el componente es exclusivo",
# MAGIC flag_familia int COMMENT "1 si todos los sku de la familia se van a racionalizar",
# MAGIC flag_familia_exclusivo int COMMENT "1 si todos los sku de la familia se van a racionalizar y no comparten componentes con otras familias",
# MAGIC flag_variedad int COMMENT "1 si todos los sku de la variedad se van a racionalizar",
# MAGIC flag_variedad_exclusivos int COMMENT "1 si todos los sku de la variedad se van a racionalizar y no comparten componentes con otras familias",
# MAGIC flag_presentacion int COMMENT "1 si todos los sku de la presentacion se van a racionalizar",
# MAGIC flag_presentacion_exclusivos int COMMENT "1 si todos los sku de la presentacion se van a racionalizar y no comparten componentes con otras familias",
# MAGIC nivel int COMMENT "Nivel de los componentes"
# MAGIC 
# MAGIC )
# MAGIC using delta
# MAGIC --partitioned by (year_month)
# MAGIC --LOCATION '/mnt/data_entities/silver/maestro_centros'

# COMMAND ----------


