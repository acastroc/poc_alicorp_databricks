# Databricks notebook source
# MAGIC %run ../../../01_utils/utils

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# Se obtiene la lista de tablas que se procesaran de raw to landing
list_table=conf_json_order()
logger = init_logging("staging")

mount='/mnt/data_s4'
raw='raw'
t_capa ='staging'
v_year=date_process('yyyy')
v_current = date_process('yyyymmddhhmmss')


# recorremos todas las tablas a procesar
for table_landing in list_table :   
    #obtiene valores de tabla
    t_table= table_landing['table']['name'].lower()
    t_partition= table_landing['table']['partition_field'].lower()
    
    t_reproceso= table_landing['reprocess']['active'].lower()
    t_dias= int(table_landing['reprocess']['days'].lower())
    
    if t_reproceso =='s' :
        logger.info(f'Procesando tabla: {t_table} - partición : {t_partition} - reproceso : {t_reproceso} - dias_reproceso : {t_dias}')
    
        t_primary_key =table_landing['primary_key']
        t_location_delta = f'{mount}/{t_capa}/{t_table}'
    
    
        print(t_reproceso,t_dias)

        if t_partition == 'd':
            partition = 'year_month_day'
        else:
            partition = 'year_month'
        
        logger.info(f'Procesando tabla: {t_table} - partición : {t_partition}')
    
        # Omitir columnas para select
        noCols = ['create_at',partition,'origin_file']
        # noCols = ['create_at']
    
        source_landing= f'landing.{t_table}'
        # Obtener columnas para Bronze y Silver
        columns = get_columns_to_select(source_landing)
        
        x = []
        x = get_partition(source_landing,partition,t_dias)
        
        for n in x:
            print("n :", n)
            df_origin = read_df_max_landing(partition,source_landing,columns,n)

            df = df_origin.drop(*noCols)
            df = df.dropDuplicates()

            logger.info('latets_landing :' +str(df.count()))

            exis_table = existe_table(f'{t_capa}',t_table)
            #logger.info(f'existe Tabla : {exis_table}')

            merge(exis_table,t_location_delta,t_capa,t_table,t_partition,df_origin,t_primary_key)
    
