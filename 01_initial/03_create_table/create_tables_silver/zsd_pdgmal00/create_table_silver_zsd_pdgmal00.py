# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.zsd_pdgmal00 ;
# MAGIC create table silver.zsd_pdgmal00
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC ZSD_NINTPA STRING,
# MAGIC ZSD_NPARTE STRING,
# MAGIC ZSD_CSOCIE STRING,
# MAGIC ZSD_CCENTR STRING,
# MAGIC ZSD_CSEDE STRING,
# MAGIC ZSD_CALMAC STRING,
# MAGIC ZSD_FPARTE STRING,
# MAGIC ZSD_XOBSER STRING,
# MAGIC ZSD_NPICKL STRING,
# MAGIC ZSD_NDCREF STRING,
# MAGIC ZSD_CTPDOC STRING,
# MAGIC ZSD_LREVER STRING,
# MAGIC ZSD_NPAREF STRING,
# MAGIC ZSD_NPAREV STRING,
# MAGIC ZSD_CMOTIV STRING,
# MAGIC ZSD_CCLAMO STRING,
# MAGIC ZSD_SPARTE STRING,
# MAGIC ZSD_CCTROR STRING,
# MAGIC ZSD_CALMOR STRING,
# MAGIC ZSD_CCTRDE STRING,
# MAGIC ZSD_CALMDE STRING,
# MAGIC ZSD_DUSCRE STRING,
# MAGIC ZSD_FCREAC STRING,
# MAGIC ZSD_DUSMOD STRING,
# MAGIC ZSD_FMODIF STRING,
# MAGIC ZSD_HCREAC STRING,
# MAGIC ZSD_HMODIF STRING,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_sx/silver/zsd_pdgmal00'
# MAGIC     
