# Databricks notebook source
# DBTITLE 1,mount : Data
#alicorp-pe-is-dsim-gcs
import os
bucket_name=os.environ['BUCKET_DATA_S4']
mount_name = "data_s4"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

#alicorp-pe-is-dsim-gcs
import os
bucket_name=os.environ['BUCKET_DATA_SX']
print(bucket_name)
mount_name = "data_sx"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

import os
bucket_name=os.environ['BUCKET_DATA_BWTPM']
print(bucket_name)
mount_name = "data_bwtpm"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

import os
bucket_name=os.environ['BUCKET_DATA_USER_FILE']
print(bucket_name)
mount_name = "data_user_file"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
#display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

import os
bucket_name=os.environ['BUCKET_DATA_ENTITIES']
mount_name = "data_entities"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

import os
bucket_name=os.environ['BUCKET_DATA_MODELS']
mount_name = "data_models"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# DBTITLE 1,mount: iview
import os
bucket_name=os.environ['BUCKET_DATA_IVIEW']
mount_name = "data_iview"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# DBTITLE 1,mount : output
#alicorp-pe-is-dsim-output-gcs #alicorp-pe-is-dsim-output-gcs
#bucket_name = "alicorp-pe-is-dsim-output-gcs"
import os
bucket_name=os.environ['BUCKET_OUTPUT']
mount_name = "output"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

#alicorp-pe-is-dsim-gcs
import os
bucket_name=os.environ['BUCKET_DATA_DG']
mount_name = "data_governance"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)

# COMMAND ----------

#alicorp-databricks-dev-gcs
import os
bucket_name=os.environ['BUCKET_DATA_DEV']
mount_name = "data_dev"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# DBTITLE 1,unmount 
#desmontar 
#dbutils.fs.unmount("/mnt/data_entities/")
#dbutils.fs.unmount("/mnt/data_iview/")

# COMMAND ----------

# DBTITLE 1,Validation 
#dbutils.fs.ls("/mnt/data/")
#dbutils.fs.ls("/mnt/output/")
#dbutils.fs.ls("/mnt/data_sx/")
dbutils.fs.ls("/mnt/")
