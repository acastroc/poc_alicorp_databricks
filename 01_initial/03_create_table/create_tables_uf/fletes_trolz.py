# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists uf.fletes_trolz ;
# MAGIC create table uf.fletes_trolz
# MAGIC (
# MAGIC   mandt string,
# MAGIC   aland string,
# MAGIC   azone string,
# MAGIC   vsbed string,
# MAGIC   tragr string,
# MAGIC   lland string,
# MAGIC   lzone string,
# MAGIC   grulg string,
# MAGIC   route string,
# MAGIC   CREATE_AT timestamp,
# MAGIC   ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION '/mnt/data_user_file/user-file/fletes_trolz'

# COMMAND ----------


