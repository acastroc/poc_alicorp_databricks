# Databricks notebook source
json_file = '''
    [
        { "table": {
                    "name": "KNA1",
                    "partition_field": "D"
                   },
         "primary_key": ["mandt","kunnr","land1"]
         }
    ]
    '''

def conf_json_order ():
    list_table = json.loads(json_file)
    return list_table

print("****** config Git *********")
