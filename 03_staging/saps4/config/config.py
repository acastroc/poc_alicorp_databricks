# Databricks notebook source
# json_file = '''
#     [
#          { "table": {
#                      "name": "TVFKT",
#                      "partition_field": "M"
#                     },
#           "primary_key": ["mandt","spras","fkart"]
#           }
#     ]
#     '''

# def conf_json_order ():
#     list_table = json.loads(json_file)
#     return list_table

# print("****** config Git *********")

# COMMAND ----------

# json_file = '''
#     [
#         { "table": {
#                     "name": "KNA1",
#                     "partition_field": "D"
#                    },
#          "primary_key": ["mandt","kunnr","land1"]
#          },
#         { "table": {
#                     "name": "VTTP",
#                     "partition_field": "D"
#                    },
#          "primary_key": ["mandt","tknum","tpnum"]
#          },
#         { "table": {
#                     "name": "TVFKT",
#                     "partition_field": "M"
#                    },
#          "primary_key": ["mandt","spras","fkart"]
#          }
#     ]
#     '''

# def conf_json_order ():
#     list_table = json.loads(json_file)
#     return list_table

# print("****** config Git *********")

# COMMAND ----------

json_file = '''
    [
        { "table": {
                    "name": "KNA1",
                    "partition_field": "D"
                   },
         "primary_key": ["mandt","kunnr","land1"],
         "reprocess": {
                     "active":"S",
                     "days":"2"
                     }
         }
    ]
    '''

def conf_json_order ():
    list_table = json.loads(json_file)
    return list_table

print("****** config Git *********")
