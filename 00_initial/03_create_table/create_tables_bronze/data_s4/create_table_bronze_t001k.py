# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t001k ;
# MAGIC create table bronze.t001k
# MAGIC (
# MAGIC mandt string ,
# MAGIC bwkey string ,
# MAGIC bukrs string ,
# MAGIC bwmod string ,
# MAGIC xbkng string ,
# MAGIC mlbwa string ,
# MAGIC mlbwv string ,
# MAGIC xvkbw string ,
# MAGIC erklaerkom string ,
# MAGIC uprof string ,
# MAGIC wbpro string ,
# MAGIC mlast string ,
# MAGIC mlasv string ,
# MAGIC bdifp string ,
# MAGIC xlbpd string ,
# MAGIC xewrx string ,
# MAGIC x2fdo string ,
# MAGIC prsfr string ,
# MAGIC mlccs string ,
# MAGIC xefre string ,
# MAGIC efrej string ,
# MAGIC fmp_prsfr string ,
# MAGIC fmp_prfrgr string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (year_month_day)
# MAGIC LOCATION '/mnt/data_s4/bronze/t001k'
# MAGIC     
