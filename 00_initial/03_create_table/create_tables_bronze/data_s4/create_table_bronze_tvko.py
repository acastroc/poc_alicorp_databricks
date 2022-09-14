# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvko ;
# MAGIC create table bronze.tvko
# MAGIC (
# MAGIC MANDT string ,
# MAGIC VKORG string ,
# MAGIC WAERS string ,
# MAGIC BUKRS string ,
# MAGIC ADRNR string ,
# MAGIC TXNAM_ADR string ,
# MAGIC TXNAM_KOP string ,
# MAGIC TXNAM_FUS string ,
# MAGIC TXNAM_GRU string ,
# MAGIC VKOAU string ,
# MAGIC KUNNR string ,
# MAGIC BOAVO string ,
# MAGIC VKOKL string ,
# MAGIC EKORG string ,
# MAGIC EKGRP string ,
# MAGIC LIFNR string ,
# MAGIC WERKS string ,
# MAGIC BSART string ,
# MAGIC BSTYP string ,
# MAGIC BWART string ,
# MAGIC LGORT string ,
# MAGIC TXNAM_SDB string ,
# MAGIC MWSKZ string ,
# MAGIC XSTCEG string ,
# MAGIC J_1ANUTIME string ,
# MAGIC MAXBI string ,
# MAGIC CREATE_AT timestamp ,
# MAGIC ORIGIN_FILE string ,
# MAGIC YEAR_MONTH_DAY string  
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvko'
# MAGIC     
