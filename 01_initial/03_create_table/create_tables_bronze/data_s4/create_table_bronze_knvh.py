# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.knvh ;
# MAGIC create table bronze.knvh
# MAGIC (
# MAGIC MANDT string,
# MAGIC HITYP string,
# MAGIC KUNNR string,
# MAGIC VKORG string,
# MAGIC VTWEG string,
# MAGIC SPART string,
# MAGIC DATAB string, 
# MAGIC DATBI string, 
# MAGIC HKUNNR string, 
# MAGIC HVKORG string, 
# MAGIC HVTWEG string, 
# MAGIC HSPART string, 
# MAGIC GRPNO string, 
# MAGIC BOKRE string,
# MAGIC PRFRE string, 
# MAGIC HZUOR string ,
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/knvh'
# MAGIC     
