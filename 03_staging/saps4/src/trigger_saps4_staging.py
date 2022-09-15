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
    
    t_primary_key =table_landing['primary_key']
    t_location_delta = f'{mount}/{t_capa}/{t_table}'
        
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
    
    df_origin = read_df_max_landing(partition,source_landing,columns)
     
    df = df_origin.drop(*noCols)
    df = df.dropDuplicates()
    
    #logger.info('latets_landing :' +str(df.count()))

    exis_table = existe_table(f'{t_capa}',t_table)
    #logger.info(f'existe Tabla : {exis_table}')
    
    if exis_table == False :
            create_table(t_location_delta,f'{t_capa}.{t_table}',t_partition,df_origin)
            logger.info(f'creacion de tabla : {t_table}')
    
    else :
        #logger.info(f'merge tabla: {t_table}')
        #logger.info(f'primary_key: {t_primary_key}')
        
        condition =''
        primary_key =''
        for field in t_primary_key :
            condition = f'{t_table}.{field} = {t_table}.{field}'
            primary_key += condition+' and '

        primary_key = primary_key[:-4]
        
        latest_query = f"""
        merge into {t_capa}.{t_table}
        using ( select {columns} from {source_landing}
                ) sqlconsult on {primary_key}
        when matchef then
        update set
            """ 
        logger.info(latest_query)
            #logger.info(latest_query)

        #df = spark.sql(latest_query)

# COMMAND ----------

display(latets_bronze)

# COMMAND ----------

#variables generales : 
bronze ='bronze'
silver ='silver'
bronze_table_name ='ztotc_costprec'
silver_table_delta = 'ztotc_costprec'
source_bronze= f'{bronze}.{bronze_table_name}'
source_silver= f'{silver}.{silver_table_delta}'
mount='/mnt/data_s4'
raw='raw'
data='data'

# metodo : date_process -> obtiene formato yyyy
v_year=date_process('yyyy')
v_current = date_process('yyyymmddhhmmss')

file_location = f'{mount}/{raw}/{bronze_table_name}/{data}/{v_year}/'
#print(file_location)
# metodo : max_file_storage -> obtiene maximo valor del archivo
max_file=max_file_storage(file_location)
#print(max_file)
name_file = max_file.get("name")

# Omitir columnas para select
noCols = ['create_at','year_month_day','origin_file']
# noCols = ['create_at']

# Obtener columnas para Bronze y Silver
columns = getColumnsToSelect(source_bronze)

#bronze query
partition1 = getPartition2(source_bronze,'year_month_day',-1)
print("partition1 :", partition1)
#bronze query
latest_query = f"""
select {columns}
from {source_bronze} 
where year_month_day = {partition1}
""" 

latets_bronze = spark.sql(latest_query)
latets_bronze = latets_bronze.drop(*noCols)
latets_bronze = latets_bronze.dropDuplicates()
print ('latets_bronze :' +str(latets_bronze.count()))


#bronze query anterior
partition2 = getPartition2(source_bronze,'year_month_day',-2)
print("partition2 :", partition2)
#bronze query
latest_query2 = f"""
select {columns}
from {source_bronze} 
where year_month_day = {partition2}
""" 

latets_bronze2 = spark.sql(latest_query2)
latets_bronze2 = latets_bronze2.drop(*noCols)
latets_bronze2 = latets_bronze2.dropDuplicates()
print ('latets_bronze2 :' +str(latets_bronze2.count()))

#delta
delta=latets_bronze.exceptAll(latets_bronze2)

#metodo : transfor_basic --> tranformacion basica 
delta=transfor_basic(delta,'basic','')

#metodo : transfor_basic --> tipo de dato decimal 
list_decimal=['prec_plan','prec_real','cost_plan','cost_real']
delta=transfor_basic(delta,'decimal',list_decimal)

#metodo : transfor_basic --> tipo de dato integer 
list_integer=['sku']
delta=transfor_basic(delta,'integer',list_integer)


if delta .count() > 0:
    print ('delta :' +str(delta.count()))
    condition = f"year_month_day='{name_file[0:8]}'"
    delta = delta\
    .withColumn('create_at', f.unix_timestamp(f.lit(v_current),'yyyy-MM-dd HH:mm:ss').cast("timestamp")) \
    .withColumn('year_month_day', f.lit(name_file[0:8])) \
    .withColumn('origin_file', f.lit(name_file))  #name_file
    delta.write.mode('overwrite').format('delta').option("replaceWhere", condition).saveAsTable(source_silver)  
    print('execution ok')
