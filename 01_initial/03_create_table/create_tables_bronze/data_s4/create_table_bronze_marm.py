# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.marm ;
# MAGIC create table bronze.marm
# MAGIC (
# MAGIC MANDT string,
# MAGIC MATNR string,
# MAGIC MEINH string,
# MAGIC UMREZ string,
# MAGIC UMREN string,
# MAGIC EANNR string,
# MAGIC EAN11 string,
# MAGIC NUMTP string,
# MAGIC LAENG string,
# MAGIC BREIT string,
# MAGIC HOEHE string,
# MAGIC MEABM string,
# MAGIC VOLUM string,
# MAGIC VOLEH string,
# MAGIC BRGEW string,
# MAGIC GEWEI string,
# MAGIC MESUB string,
# MAGIC ATINN string,
# MAGIC MESRT string,
# MAGIC XFHDW string,
# MAGIC XBEWW string,
# MAGIC KZWSO string,
# MAGIC MSEHI string,
# MAGIC BFLME_MARM string,
# MAGIC GTIN_VARIANT string,
# MAGIC NEST_FTR string,
# MAGIC MAX_STACK string,
# MAGIC TOP_LOAD_FULL string,
# MAGIC TOP_LOAD_FULL_UOM string,
# MAGIC CAPAUSE string,
# MAGIC TY2TQ string,
# MAGIC DUMMY_UOM_INCL_EEW_PS string,
# MAGIC CWM_TY2TQ string,
# MAGIC STTPEC_NCODE string,
# MAGIC STTPEC_NCODE_TY string,
# MAGIC STTPEC_RCODE string,
# MAGIC STTPEC_SERUSE string,
# MAGIC STTPEC_SYNCCHG string,
# MAGIC STTPEC_SERNO_MANAGED string,
# MAGIC STTPEC_SERNO_PROV_BUP string,
# MAGIC STTPEC_UOM_SYNC string,
# MAGIC PCBUT string,
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/marm'
# MAGIC     
