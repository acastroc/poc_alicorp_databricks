# Databricks notebook source
# MAGIC %run ../../../01_utils/utils

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

list_table=conf_json_order()
logger = init_logging("Landing")

mount='/mnt/data_s4'
raw='raw'
t_capa ='landing'
v_year=date_process('yyyy')

for table_landing in list_table :   
    #obtiene valores de tabla
    t_table= table_landing['table']['name'].lower()
    t_partition= table_landing['table']['partition_field'].lower()
    
    t_reproceso= table_landing['reprocess']['active'].lower()
    t_dias= table_landing['reprocess']['days'].lower()
    
    if t_reproceso =='s' :
        logger.info(f'Procesando tabla: {t_table} - partición : {t_partition} - reproceso : {t_reproceso} - dias_reproceso : {t_dias}')
        #se define la ruta de carpeta raw
        t_location = f'{mount}/{raw}/{t_table}/data/{v_year}/'
        #se define la ruta de la tabla delta
        t_location_delta = f'{mount}/{t_capa}/{t_table}'
        
        #se obtiene maximo archivo a procesar
        max_file=max_file_storage(t_location)
        name_file = max_file.get("name")
        date_file = max_file.get("date")
        
        for n in range(int(t_dias)):
            try:
                #se obtiene el archivo a procesar dentro de raw
                file_location_csv= f'{t_location}{name_file}'
                new_date = date_file - timedelta(days=n)
                # obtiene el año
                v_year = new_date.strftime("%Y")
                # obtiene el mes
                v_month = new_date.strftime("%m")
                # obtiene el día
                v_day = new_date.strftime("%d")
                
                
                v_name_file = f'{v_year}{v_month}{v_day}.csv'  
                file_location_csv = f'{mount}/{raw}/{t_table}/data/{v_year}/{v_name_file}'
                
                
                logger.info(file_location_csv)
                
                parameter ={ 'file_location_csv':file_location_csv,
                     'name_file':v_name_file,
                     't_partition':t_partition,
                     't_location_delta':t_location_delta,
                     't_format':'reproces',
                     't_day':t_dias
                }
                
                #logger.info(f'{parameter}')
                #se graba el df en formato delta en el storage
                process= save_df_schedule(parameter)
                #logger.info(f'{process}')
        
            except Exception as e:
                #Email("la capa Bronze de Customers_Hierarchy", str(e))
                logger.info(f'Error : {t_table}')    
