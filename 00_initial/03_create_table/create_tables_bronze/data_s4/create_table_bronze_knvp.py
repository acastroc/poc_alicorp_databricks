# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.knvp ;
# MAGIC create table bronze.knvp
# MAGIC (
# MAGIC MANDT string,
# MAGIC KUNNR string,
# MAGIC VKORG string,
# MAGIC VTWEG string,
# MAGIC SPART string,
# MAGIC PARVW string,
# MAGIC PARZA string,
# MAGIC KUNN2 string,
# MAGIC LIFNR string,
# MAGIC PERNR string,
# MAGIC PARNR string,
# MAGIC KNREF string,
# MAGIC DEFPA string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/knvp'
# MAGIC     
