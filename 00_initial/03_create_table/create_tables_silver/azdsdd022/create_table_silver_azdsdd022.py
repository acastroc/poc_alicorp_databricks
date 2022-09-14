# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.azdsdd022 ;
# MAGIC create table silver.azdsdd022
# MAGIC (
# MAGIC DIVISION string
# MAGIC ,CUSTOMER string
# MAGIC ,DISTR_CHAN string
# MAGIC ,SALESORG string
# MAGIC ,CALMONTH string
# MAGIC ,CALWEEK string
# MAGIC ,CURRENCY string
# MAGIC ,PROD_ID string
# MAGIC ,RECORDMODE string
# MAGIC ,CALYEAR string
# MAGIC ,FISCYEAR string
# MAGIC ,FISCPER string
# MAGIC ,FISCVARNT string
# MAGIC ,BICZRLIST DECIMAL(22,3)
# MAGIC ,BICZRCOGS DECIMAL(22,3)
# MAGIC ,BJBPBS_RCOGS_S DECIMAL(22,3)
# MAGIC ,BJBPBS_RLIST_S DECIMAL(22,3)
# MAGIC 
# MAGIC 
# MAGIC ,CREATE_AT timestamp
# MAGIC ,YEAR_MONTH_DAY string
# MAGIC ,ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_bwtpm/silver/azdsdd022'
# MAGIC     
