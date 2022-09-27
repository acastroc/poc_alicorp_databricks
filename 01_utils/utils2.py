# Databricks notebook source
from datetime import datetime,timedelta
from delta.tables import *
from pyspark.sql import DataFrame
from typing import List
from google.cloud import secretmanager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import storage
from pyspark.sql.types import  *
import os,requests,json,base64,time,smtplib
import pyspark.sql.functions as f 
import yaml as yml
import logging
from pyspark.sql import SQLContext

sqlContext = SQLContext(spark.sparkContext)

def read_df_max_landing (partition:str,source_landing:str,columns:str) ->DataFrame:
    
    latest_query = f"""
    with
    filter_tables as ( 
      select 
       max({partition}) as particion , max(CREATE_AT) as CREATE_AT
      from {source_landing} 
      where {partition} in (select max({partition}) from {source_landing} )
    ),
    table_landing(
    select * from {source_landing}
    where {partition} = (select particion from filter_tables)
    and CREATE_AT >= (select CREATE_AT from filter_tables)
    )
    select {columns} from table_landing
    """ 
    
    #logger.info(latest_query)
    
    df = spark.sql(latest_query)
    
    
    return df 

def create_table (save_path:str,table_name:str,t_partition:str,df:DataFrame):
    """ 
    Definición :  
        Creacion de tabla de forma automatica
    Parámetros:
        save_path (str): ruta de la tabla en storage
        table_name (str): nombre de la tabla 
    Resultado:
        
    """
    partition = ''
    
    if t_partition == 'd':
        partition = 'year_month_day'
    else:
        partition = 'year_month'
        
       
    list = []
    listColsDelta = ''
    for col in df.dtypes:
        listColsDelta = col[0]+" "+col[1]
        list.append(listColsDelta)
    
    metadata = "("+','.join(list)+")"
    
        
    spark.sql(f" drop table if exists {table_name}")
    # spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")
    spark.sql(f" create table {table_name} {metadata} using delta partitioned by ({partition}) location '{save_path}'" )

######## funcion max_file_storage
def existe_table (dataBase:str,table:str)-> bool:
    """ 
    Definición :  
        Validamos si existe tabla
    Parámetros:
        dataBase (str): base datos
        table_name (str): tabla  
    Resultado:
        bool : True si existe / False no existe
    """
    table_names_in_db = sqlContext.tableNames(dataBase)
    table_exists = table in table_names_in_db
    return table_exists
    
    
######## funcion max_file_storage
def max_file_storage (path_storage:str)-> dict:
    """ 
    Definición :  
        Metodo que retonar el maximo valor del archivo que se encuentra en el storage
    Parámetros:
        str1 (str): ruta donde buscara la maxima fecha
    Resultado:
        dict : diccionario con nombre,fecha en date y string
    """
    # valor inicial de busqueda
    val= datetime(1999,1,1,0,0,0)
    list=[]
    list = dbutils.fs.ls(path_storage)
    for list_process_csv in range(len(list)):
        name=list[list_process_csv][1]
        # day = f'{name[0:4]}-{name[4:6]}-{name[6:8]} {name[9:11]}:{name[11:13]}:{name[13:15]}'
        # date = datetime.strptime(day, '%Y-%m-%d %H:%M:%S')
        day = f'{name[0:8]}'
        date = datetime.strptime(day, '%Y%m%d')
        filter = f'{name[0:15]}'
        if date >= val:
            val=date
            dict_file = {'name': name,'day': day,'date':date,'filter':filter}
    return dict_file

######## funcion setup_logging
def init_logging(name):
    """ 
    Definición : 
        Init logging settings
    Parámetros:
                name (str): Logger name
    Resultado: 
                logger: logger instance
    """
    MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    return logger

######## funcion setup_logging
def date_process(var:str)->str:
    """ 
    Definición :  
        Metodo para guardar e imprimir los logs 
    Parámetros:
        cadena de busqueda para la fecha 
    Resultado:
        dict : diccionario con nombre,fecha en date y string
    """
    now = datetime.now()
    date_now=now-timedelta(hours=5)

    year = '{:02d}'.format(date_now.year)
    month = '{:02d}'.format(date_now.month)
    day = '{:02d}'.format(date_now.day)
    
    if var == 'yyyymm':
        current = '{}{}'.format(year,month)
    elif var == 'yyyy' :
        current = '{}'.format(year)
    elif var=='yyyymmdd':
        current = '{}{}{}'.format(year,month,day)
    elif var=='yyyymmddhhmmss':
        current = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    
    #current='20211019'
    return current


######## transfor_basic
def transfor_basic (df:DataFrame,typeTrans:str,listVal:List)->DataFrame:
    """
    Definición :  
        transformacion basica para df
    Parámetros:
       df : Dataframe de la capa bronce a silver 
    Resultado:
        df : Dataframe con las transformaciones Basicas 
    """
    # transformation lower title
    if typeTrans == 'basic':
        for x in df.schema.names:
            df=df.withColumnRenamed(x,x.lower())
        for x in df.schema.names:
            df=df.withColumn(x,f.upper(f.trim(f.col(x))))
        
        
    if typeTrans == 'integer':
        for x in listVal:
            #print(x)
            df = df.withColumn(x.lower(),f.col(x).cast("integer"))
    
    if typeTrans == 'timestamp':
        for x in listVal:
            df = df.withColumn(x.lower(),f.to_date(f.unix_timestamp(f.col(x), "yyyymmdd").cast("timestamp")))
    
    if typeTrans == 'decimal':
        for x in listVal:
            df = df.withColumn(x.lower(),f.regexp_replace(latets_bronze[x],' ','').cast('decimal(22,3)'))
            
    if typeTrans == 'double':
        for x in listVal:
            df = df.withColumn(x.lower(),f.regexp_replace(latets_bronze[x],' ','').cast('double'))

    return df

 
    
def get_columns_to_select(sourceTable:str)->List:
    """
    Definición :  
        Devuelve los select necesarios en cada capa para la tranformacion
    Parámetros:
       nombre de la tabla  
    Resultado:
        Lista de campos
    """
    listColsDelta = spark.sql("select * from " + sourceTable + " limit 1").schema.names
    columnsToSelect = ','.join(listColsDelta)
    
    return columnsToSelect.lower()

def get_date()->datetime:
    """ 
    Definición :  
        Devuelve fecha en formato date
    Parámetros:
       sin Parámetros
    Resultado:
        fecha
    """
    
    now = datetime.now()
    stringDate = now.strftime("%d-%m-%Y")
    date = datetime.strptime(stringDate, '%d-%m-%Y').date()
    return date
  

def validate_date(df:DataFrame)->DataFrame:
    """
    Definición :  
        validacion de fecha en caso envie null 
    Parámetros:
       df 
    Resultado:
        df con validacion de campos
    """
    col_list = df.dtypes

    for x,y in col_list:
        if y == 'date':
            df = df.withColumn(x, 
                f.when(df[x] < '1900-01-01', f.to_date(f.lit(None), "yyyy-MM-dd")).
                otherwise(df[x]))
        if y == 'timestamp':
            df = df.withColumn(x, 
                f.when(df[x] < '1900-01-01', f.to_timestamp(f.lit(None), "yyyy-MM-dd HH:mm:ss.S")).
                otherwise(df[x]))
            
    return df
  
# def getEmailCredentials():
#     dsimCredenciales = eval(dbutils.secrets.getBytes(scope="alicorp_dsim", key="SECRET"))
#     return dsimCredenciales

def get_email_credentials():
    """"
    Definición :  
        metodo de ayuda para el envio de correo
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.environ['SECRET_PROJECT']}/secrets/{os.environ['SECRET_SERVICE']}/versions/{os.environ['SECRET_VERSION']}"
    response = client.access_secret_version(request={'name':name})
    payload = response.payload.data.decode('utf-8')

    env_vars = yml.full_load(stream=payload)
    
    return env_vars
    

class Email:
    #deinition :  
    #    metodo de ayuda para el envio de correo
    
      def __init__(self,filename, error):
        secret = get_email_credentials()
        self.server = smtplib.SMTP(host='smtp.gmail.com',port='587')
        html = f'''
          <html>
            <body>
              <p>
                Estimados,<br>
                hubo un problema en {filename}, por favor revisar el log.<br><br>
                Detalle: {error}
              </p>
            </body>
          </html>
        '''
        self.html = MIMEText(html,'html')
        self.connect(secret)
        self.send_message(secret)
    
      def connect(self,secret):
        self.server.ehlo()
        self.server.starttls()
        self.server.login(secret['EMAIL_FROM'],secret['EMAIL_PASS'])
    
      def send_message(self,secret):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = secret['EMAIL_SUBJECT'] 
        msg['From'] = secret['EMAIL_FROM'] 
        msg['To'] = secret['EMAIL_TO'] 
        msg.attach(self.html)
        self.server.send_message(msg)

# dbutils.secrets.listScopes()
# dbutils.secrets.list("alicorp_dsim")
# my_secret = dbutils.secrets.getBytes(scope="alicorp_dsim", key="SECRET")
# my_secret.decode("utf-8")

# x = eval(my_secret)
# email_from = x['EMAIL_FROM']
# email_pass = x['EMAIL_PASS']
# print(email_from,email_pass)


def read_yaml(bucket:str,file:str)-> yml:
    """
    Definición :  
        metodo para obtener un archivo yaml o un archivo json 
    Parámetros:
       ruta del archivo y el bucket
    Resultado:
        archivo que se encuentra en storage 
    """
    bucket_gcp = ''
    
    if(bucket=='data_s4'):
        bucket_gcp=os.environ['BUCKET_DATA_S4']
    elif(bucket=='data_sx'):
        bucket_gcp=os.environ['BUCKET_DATA_SX']
    elif(bucket=='data_bwtpm'):
        bucket_gcp=os.environ['BUCKET_DATA_BWTPM']
    elif(bucket=='data_entities'):
        bucket_gcp=os.environ['BUCKET_DATA_ENTITIES']
    elif(bucket=='data_iview'):
        bucket_gcp=os.environ['BUCKET_DATA_IVIEW']
    else:
        bucket_gcp = ''
    #print("bucket : ", bucket_gcp)
    # bucket_gcp=os.environ['BUCKET_DATA_S4']
    storage_client = storage.Client()
    bucket_name = bucket_gcp  # Do not put 'gs://my_bucket_name'
    bucket = storage_client.get_bucket(bucket_gcp)
    blob = bucket.get_blob(file)
    downloaded_file = blob.download_as_string()
    downloaded_yaml_file = yml.safe_load(blob.download_as_text(encoding="utf-8"))
    return downloaded_yaml_file 
     
######## funcion save datos delta
def save_df_schedule (parameter:json,logger)->str:
    """ 
    Definición :  
        Metodo que retonar el maximo valor del archivo que se encuentra en el storage
    Parámetros:
        str1 (str): ruta donde buscara la maxima fecha
    Resultado:
        dict : diccionario con nombre,fecha en date y string
    """
    
    now = datetime.now()
    date_current = now - timedelta(hours=5)
    day = '{:02d}'.format(date_current.day)
    
    # valor inicial de busqueda
    file_type = 'csv'
    infer_schema = 'false'
    first_row_is_header = 'true'
    delimiter = ','
    v_current = date_process('yyyymmddhhmmss')
    
    
    file_location_csv = parameter['file_location_csv']
    
    name_file = parameter['name_file']
    partition = parameter['t_partition']
    path_delta = parameter['t_location_delta']
    t_format = parameter['t_format']
    list_day = parameter['t_day']
                        
    
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("multiline", "true") \
        .option("encoding", "utf8") \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location_csv)

    df = df \
        .withColumn('CREATE_AT', f.unix_timestamp(f.lit(v_current), 'yyyy-MM-dd HH:mm:ss').cast("timestamp")) \
        .withColumn('ORIGIN_FILE', f.lit(name_file))
    
    if partition == 'm':
        df = df.withColumn('year_month', f.lit(name_file[0:6]))
    else:
        df = df.withColumn('year_month_day', f.lit(name_file[0:8]))
       
    for each in df.columns:
        df = df.withColumnRenamed(each , each.strip())
        
    if (t_format == 'month' ):
        logger.info('tabla mensual sin procesar')
        if day in list_day :
                df.write.mode('append').format('delta').partitionBy("year_month").save(path_delta)
                logger.info('tabla mensual con fecha de carga')
    elif (t_format == 'daily')  :
        df.write.mode('append').format('delta').partitionBy("year_month_day").save(path_delta)
        logger.info('tabla diaria')
    elif (t_format == 'reproces')  :
        df.write.mode('append').format('delta').save(path_delta)
        logger.info('reproceso ')
    else :
        logger.info('Sin reproceso')
  
    return df
 
def get_partition(table:str, part, cnt):
    
    """ 
    Definición :  
        Metodo que retonar el maximo valor del archivo que se encuentra en el storage
    Parámetros:
        str1 (str): ruta donde buscara la maxima fecha
    Resultado:
        dict : diccionario con nombre,fecha en date y string
    """
    
    try:
        df = spark.sql(f"show partitions {table}")
        partition = df.select(part).distinct().collect()
        partition = [r[part] for r in partition]
        partition.sort()
        partition = partition[cnt]
        return partition
    except Exception as x:
        return 0
    
    
    
def merge(exis_table:bool,t_location_delta:str,t_capa:str,t_table:str,t_partition:str,df_origin:DataFrame,t_primary_key:str):
    if exis_table == False :
        create_table(t_location_delta,f'{t_capa}.{t_table}',t_partition,df_origin)
        staging = f'{t_capa}.{t_table}'
        df_origin.write.mode('overwrite').format('delta').saveAsTable(staging) 
        logger.info(f'creacion de tabla : {t_table}')
    else :
        logger.info(f'merge tabla: {t_table}')
        logger.info(f'primary_key: {t_primary_key}')
        
        #######################
        #### condition
        #######################
        condition =''
        primary_key =''
        for field in t_primary_key :
            condition = f'{t_table}.{field} = {t_table}.{field}'
            primary_key += condition+' and '

        primary_key = primary_key[:-4]
        
        #######################
        #### update 
        #######################
        
        lista_columns_update=columns.split(",")
        update_merge=''
        list_update_merge =''
        
        for remove_item in t_primary_key :
            lista_columns_update.remove(remove_item)

        for fields_update in lista_columns_update:
            update_merge= f'{t_table}.{fields_update}= sqlconsult.{fields_update},'
            list_update_merge += update_merge 
    
        list_update_merge = list_update_merge[:-1]
        
        #######################
        #### insert 
        #######################
        
        lista_columns_insert=columns.split(",")
        
        insert_merge=''
        list_insert_merge =''
        
        for fields_insert in lista_columns_insert:
            insert_merge= f'sqlconsult.{fields_insert},'
            list_insert_merge += insert_merge 
        
        list_insert_merge = list_insert_merge[:-1]
        
        #######################
        #### script merge
        #######################
        latest_query = f"""
        merge into {t_capa}.{t_table}
        using ( 
                 select {columns} from {source_landing} 
                 where {partition} in ( select max(create_at) from {source_landing} )
                 and create_at >= (select max(create_at) from {source_landing} )
                  
                ) sqlconsult on {primary_key}
        when matched then
        update set {list_update_merge}
        
        when not matched then
        insert ({columns})
        values ({list_insert_merge})
        """ 
        #logger.info(latest_query)
        logger.info(latest_query)
        spark.sql(latest_query)
    
    
print("****** Version Git *********")

