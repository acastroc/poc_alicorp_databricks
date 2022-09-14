# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.stko ;
# MAGIC create table bronze.stko
# MAGIC (
# MAGIC MANDT string,
# MAGIC STLTY string,
# MAGIC STLNR string,
# MAGIC STLAL string,
# MAGIC STKOZ string,
# MAGIC DATUV string,
# MAGIC TECHV string,
# MAGIC AENNR string,
# MAGIC LKENZ string,
# MAGIC LOEKZ string,
# MAGIC VGKZL string,
# MAGIC ANDAT string,
# MAGIC ANNAM string,
# MAGIC AEDAT string,
# MAGIC AENAM string,
# MAGIC BMEIN string,
# MAGIC BMENG string,
# MAGIC CADKZ string,
# MAGIC LABOR string,
# MAGIC LTXSP string,
# MAGIC STKTX string,
# MAGIC STLST string,
# MAGIC WRKAN string,
# MAGIC DVDAT string,
# MAGIC DVNAM string,
# MAGIC AEHLP string,
# MAGIC ALEKZ string,
# MAGIC GUIDX string,
# MAGIC VALID_TO string,
# MAGIC ECN_TO string,
# MAGIC BOM_VERSN string,
# MAGIC VERSNST string,
# MAGIC VERSNLASTIND string,
# MAGIC LASTCHANGEDATETIME string,
# MAGIC BOM_AIN_IND string,
# MAGIC BOM_PREV_VERSN string,
# MAGIC DUMMY_STKO_INCL_EEW_PS string,
# MAGIC CREATE_AT timestamp,
# MAGIC ORIGIN_FILE string,
# MAGIC YEAR_MONTH_DAY string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/stko'
# MAGIC     
