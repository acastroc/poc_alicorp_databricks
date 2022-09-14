# Databricks notebook source
# MAGIC %run ../../../utils/utils

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# Se obtiene la lista de tablas que se procesaran de raw to landing
list_table=conf_json_order()
logger = init_logging("Landing")

mount='/mnt/data_s4'
raw='raw'
t_capa ='landing'
v_year=date_process('yyyy')

# recorremos todas las tablas a procesar
for table_landing in list_table :   
    #obtiene valores de tabla
    t_table= table_landing['table']['name'].lower()
    t_partition= table_landing['table']['partition_field'].lower()
    
    logger.info(f'Procesando tabla: {t_table} - partición : {t_partition}')
    
    #obtiene el schedule
    t_format= table_landing['schedule']['format'].lower()
    t_day= table_landing['schedule']['day']
    
    #se define la ruta de carpeta raw
    t_location = f'{mount}/{raw}/{t_table}/data/{v_year}/'
    #se define la ruta de la tabla delta
    t_location_delta = f'{mount}/{t_capa}/{t_table}'
    
    try:
        #se obtiene maximo archivo a procesar
        max_file=max_file_storage(t_location)
        name_file = max_file.get("name")
        
        #se obtiene el archivo a procesar dentro de raw
        file_location_csv= f'{t_location}{name_file}'
        logger.info(f'file_location_csv : {file_location_csv} - name_file : {name_file}')
        
        parameter ={ 'file_location_csv':file_location_csv,
                     'name_file':name_file,
                     't_partition':t_partition,
                     't_location_delta':t_location_delta,
                     't_format':t_format,
                     't_day':t_day
        }
        #se graba el df en formato delta en el storage
        process= save_df_schedule(parameter)
        logger.info(f'{process}')
        
        #Validamos si existe la tabla creada en databricks
        exis_table = existe_table('landing',t_table)
        
        if exis_table == False :
            create_table(t_location_delta,f'{t_capa}.{t_table}')
            logger.info(f'Creacion de tabla : {t_table}')
    
    except Exception as e:
        #Email("la capa Bronze de Customers_Hierarchy", str(e))
        logger.info(f'Error : {t_table}')
