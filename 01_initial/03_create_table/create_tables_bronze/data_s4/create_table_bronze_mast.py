# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.mast ;
# MAGIC create table bronze.mast
# MAGIC (
# MAGIC mandt string ,
# MAGIC matnr string ,
# MAGIC werks string ,
# MAGIC stlan string ,
# MAGIC stlnr string ,
# MAGIC stlal string ,
# MAGIC losvn string ,
# MAGIC losbs string ,
# MAGIC andat string ,
# MAGIC annam string ,
# MAGIC aedat string ,
# MAGIC aenam string ,
# MAGIC cslty string ,
# MAGIC material_bom_key string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/mast'
# MAGIC     
