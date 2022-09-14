# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t001l ;
# MAGIC create table bronze.t001l
# MAGIC (
# MAGIC mandt string ,
# MAGIC werks string ,
# MAGIC lgort string ,
# MAGIC lgobe string ,
# MAGIC spart string ,
# MAGIC xlong string ,
# MAGIC xbufx string ,
# MAGIC diskz string ,
# MAGIC xblgo string ,
# MAGIC xress string ,
# MAGIC xhupf string ,
# MAGIC parlg string ,
# MAGIC vkorg string ,
# MAGIC vtweg string ,
# MAGIC vstel string ,
# MAGIC lifnr string ,
# MAGIC kunnr string ,
# MAGIC mesbs string ,
# MAGIC messt string ,
# MAGIC oih_licno string ,
# MAGIC oig_itrfl string ,
# MAGIC oib_tnkassign string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t001l'
# MAGIC     
