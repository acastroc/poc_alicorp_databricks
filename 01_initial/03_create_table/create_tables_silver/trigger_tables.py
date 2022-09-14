# Databricks notebook source
# MAGIC %run ../../../CommonFunction/utils/

# COMMAND ----------

buckets = ['data_entities']
  ## Debe ir dentro del for b in buckets
for b in buckets:
    cnt = 1
    file_yaml = read_yaml(b,'config/list_tables_to_ingest_silver.json')
    tables_load=file_yaml['tables']
    #print(tables_load)
    tables_lst = []
    for x in tables_load:
        columnsToSelect = (x['name']).lower()
        try:
            #print({tables_lst[cnt]})
            print("tabla: ",columnsToSelect)
            dbutils.notebook.run(f"./{columnsToSelect}/create_table_silver_{columnsToSelect}", 10000)
            cnt = cnt + 1
        except Exception as e:
            Email("Error en capa Silver", str(e))
            print("exception ", e)
            pass
    
