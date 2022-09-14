# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists gold.zsd_tmaxal00 ;
# MAGIC create table gold.zsd_tmaxal00
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC ZSD_CSOCIE STRING,
# MAGIC ZSD_CSEDE STRING,
# MAGIC ZSD_CCENTR STRING,
# MAGIC ZSD_CALMAC STRING,
# MAGIC ZSD_CMATER STRING,
# MAGIC ZSD_ICOPRO DECIMAL(22,3),
# MAGIC ZSD_ICOTOT DECIMAL(22,3),
# MAGIC ZSD_QSTDIS STRING,
# MAGIC ZSD_QSTCOM STRING,
# MAGIC ZSD_QSTCCA STRING,
# MAGIC ZSD_QSTCMP STRING,
# MAGIC ZSD_CMONED STRING,
# MAGIC ZSD_CUNMED STRING,
# MAGIC ZSD_QSTRES STRING,
# MAGIC ZSD_ICOTOG STRING,
# MAGIC ZSD_FMODIF STRING,
# MAGIC ZSD_HMODIF STRING,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_sx/gold/zsd_tmaxal00'
# MAGIC     
