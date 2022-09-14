# Databricks notebook source
# MAGIC %run ../../../CommonFunction/utils

# COMMAND ----------

# buckets = ['data_s4','data_bwtpm','data_sx','data_isv']
buckets = ['data_sx']
#tables_lst = []
  ## Debe ir dentro del for b in buckets
for b in buckets:
    print("bucket : ", b)
    cnt = 0
    file_yaml = read_yaml(b,'config/list_tables_to_ingest.json')
    tables_load=file_yaml['tables']
    #print(tables_load)
    tables_lst=[]
    for x in tables_load:
        columnsToSelect = (x['name']).lower()
        tables_lst.append(columnsToSelect)
        # RUN SCRIPT CREATE TABLES
        try:
            print("table : " ,tables_lst[cnt])
            dbutils.notebook.run(f"./{b}/create_table_bronze_{tables_lst[cnt]}", 10000)
            cnt = cnt + 1
        except Exception as e:
            Email("la capa Bronze de Customers_Hierarchy", str(e))
            print("exception ", e)
            pass
    
