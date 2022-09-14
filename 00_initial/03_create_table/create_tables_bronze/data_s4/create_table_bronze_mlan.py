# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.mlan ;
# MAGIC create table bronze.mlan
# MAGIC (
# MAGIC mandt string ,
# MAGIC matnr string ,
# MAGIC aland string ,
# MAGIC taxm1 string ,
# MAGIC taxm2 string ,
# MAGIC taxm3 string ,
# MAGIC taxm4 string ,
# MAGIC taxm5 string ,
# MAGIC taxm6 string ,
# MAGIC taxm7 string ,
# MAGIC taxm8 string ,
# MAGIC taxm9 string ,
# MAGIC taxim string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_day)
# MAGIC LOCATION '/mnt/data_s4/bronze/mlan'
# MAGIC     
