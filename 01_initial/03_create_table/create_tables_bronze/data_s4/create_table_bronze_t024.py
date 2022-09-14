# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t024 ;
# MAGIC create table bronze.t024
# MAGIC (
# MAGIC mandt string ,
# MAGIC ekgrp string ,
# MAGIC eknam string ,
# MAGIC ektel string ,
# MAGIC ldest string ,
# MAGIC telfx string ,
# MAGIC tel_number string ,
# MAGIC tel_extens string ,
# MAGIC smtp_addr string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t024'
# MAGIC     
