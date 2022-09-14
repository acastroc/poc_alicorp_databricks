# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.stas ;
# MAGIC create table bronze.stas
# MAGIC (
# MAGIC MANDT string,
# MAGIC STLTY string,
# MAGIC STLNR string,
# MAGIC STLAL string,
# MAGIC STLKN string,
# MAGIC STASZ string,
# MAGIC DATUV string,
# MAGIC TECHV string,
# MAGIC AENNR string,
# MAGIC LKENZ string,
# MAGIC ANDAT string,
# MAGIC ANNAM string,
# MAGIC AEDAT string,
# MAGIC AENAM string,
# MAGIC DVDAT string,
# MAGIC DVNAM string,
# MAGIC AEHLP string,
# MAGIC STVKN string,
# MAGIC IDPOS string,
# MAGIC IDVAR string,
# MAGIC LPSRT string,
# MAGIC BOM_VERSN string,
# MAGIC CREATE_AT timestamp,
# MAGIC ORIGIN_FILE string,
# MAGIC YEAR_MONTH_DAY string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/stas'
# MAGIC     
