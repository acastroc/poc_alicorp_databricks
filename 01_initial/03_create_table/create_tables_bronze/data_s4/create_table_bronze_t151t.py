# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t151t ;
# MAGIC create table bronze.t151t
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC KDGRP string,
# MAGIC KTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAy)
# MAGIC LOCATION '/mnt/data_s4/bronze/t151t'
# MAGIC     
