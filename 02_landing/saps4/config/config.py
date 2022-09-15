# Databricks notebook source
json_file = '''
    [
        { "table": {
                    "name": "KNA1",
                    "partition_field": "D"
                   },
          "reprocess": {
                     "active":"S",
                     "days":"5"
                     },
         "schedule": {
                     "format":"daily",
                     "day":[]
                    }
         },
         { "table": {
                    "name": "TVFKT",
                    "partition_field": "M"
                   },
          "reprocess": {
                     "active":"S",
                     "days":"2"
                     },
         "schedule": {
                     "format":"month",
                     "day":["04","15","30","14"]
                    }
         },
         { "table": {
                    "name": "KNVV",
                    "partition_field": "M"
                   },
          "reprocess": {
                     "active":"S",
                     "days":"5"
                     },
         "schedule": {
                     "format":"month",
                     "day":["04","15","30","14"]
                    }
         },
         { "table": {
                    "name": "VTTP",
                    "partition_field": "D"
                   },
          "reprocess": {
                     "active":"N",
                     "days":"0"
                     },
         "schedule": {
                     "format":"daily",
                     "day":[]
                    }
         }
    ]
    '''

def conf_json_order ():
    list_table = json.loads(json_file)
    return list_table

print("****** config Git *********")
