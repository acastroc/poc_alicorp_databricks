# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tactics ;
# MAGIC create table bronze.tactics
# MAGIC (
# MAGIC CLIENT STRING
# MAGIC ,TACTIC_TYPE STRING
# MAGIC ,TACTIC STRING
# MAGIC ,TACTIC_DESC STRING
# MAGIC ,TACTIC_SHORT_DES STRING
# MAGIC ,TACT_GROUP_TYPE STRING
# MAGIC ,TACTIC_TYPE_DESC STRING
# MAGIC ,TACTIC_TYPE_S_DE STRING
# MAGIC ,CREATE_AT timestamp
# MAGIC ,YEAR_MONTH_DAY string
# MAGIC ,ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_bwtpm/bronze/tactics'
# MAGIC     
