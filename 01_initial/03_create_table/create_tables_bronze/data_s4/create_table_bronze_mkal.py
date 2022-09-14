# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.mkal ;
# MAGIC create table bronze.mkal
# MAGIC (
# MAGIC MANDT  string,
# MAGIC MATNR  string,
# MAGIC WERKS  string,
# MAGIC VERID  string,
# MAGIC BDATU  string,
# MAGIC ADATU  string,
# MAGIC STLAL  string,
# MAGIC STLAN  string,
# MAGIC PLNTY  string,
# MAGIC PLNNR  string,
# MAGIC ALNAL  string,
# MAGIC BESKZ  string,
# MAGIC SOBSL  string,
# MAGIC LOSGR  string,
# MAGIC MDV01  string,
# MAGIC MDV02  string,
# MAGIC TEXT1  string,
# MAGIC EWAHR  string,
# MAGIC VERTO  string,
# MAGIC SERKZ  string,
# MAGIC BSTMI  string,
# MAGIC BSTMA  string,
# MAGIC RGEKZ  string,
# MAGIC ALORT  string,
# MAGIC PLTYG  string,
# MAGIC PLNNG  string,
# MAGIC ALNAG  string,
# MAGIC PLTYM  string,
# MAGIC PLNNM  string,
# MAGIC ALNAM  string,
# MAGIC CSPLT  string,
# MAGIC MATKO  string,
# MAGIC ELPRO  string,
# MAGIC PRVBE  string,
# MAGIC PRFG_F  string,
# MAGIC PRDAT  string,
# MAGIC MKSP  string,
# MAGIC PRFG_R  string,
# MAGIC PRFG_G  string,
# MAGIC PRFG_S  string,
# MAGIC UCMAT  string,
# MAGIC VERSIND  string,
# MAGIC PPEGUID  string,
# MAGIC CREATE_AT timestamp ,
# MAGIC ORIGIN_FILE  string,
# MAGIC YEAR_MONTH_DAY  string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/mkal'
# MAGIC     
