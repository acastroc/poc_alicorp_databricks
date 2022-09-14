# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.zsd_tprobo00 ;
# MAGIC create table bronze.zsd_tprobo00
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC ZSD_CENTOR STRING,
# MAGIC ZSD_CPROMO STRING,
# MAGIC ZSD_CTPOFE STRING,
# MAGIC ZSD_TIPPRO STRING,
# MAGIC ZSD_CGRPMA STRING,
# MAGIC ZSD_NRANGO STRING,
# MAGIC ZSD_SPROBO STRING,
# MAGIC ZSD_LRESPR STRING,
# MAGIC ZSD_DUSCRE STRING,
# MAGIC ZSD_FCREAC STRING,
# MAGIC ZSD_HCREAC STRING,
# MAGIC ZSD_DUSMOD STRING,
# MAGIC ZSD_FMODIF STRING,
# MAGIC ZSD_LOBES STRING,
# MAGIC ZSD_CGRPCA STRING,
# MAGIC ZSD_LVETE STRING,
# MAGIC ZSD_TDOCV STRING,
# MAGIC ZSD_OITG STRING,
# MAGIC ZSD_PRPAG STRING,
# MAGIC ZSD_TRDCTO STRING,
# MAGIC ZSD_ZMADCTO STRING,
# MAGIC ZSD_TPROBO00 STRING,
# MAGIC ZSD_IOBJT STRING,
# MAGIC ZSD_CTPOB STRING,
# MAGIC ZSD_DUSSOL STRING,
# MAGIC ZSD_FECCOM STRING,
# MAGIC ZSD_HORCOM STRING,
# MAGIC ZSD_LTEST STRING,
# MAGIC ZSD_BUKRS STRING,
# MAGIC ZSD_TIPROMO STRING,
# MAGIC CHKSTK STRING,
# MAGIC MAXVECES STRING,
# MAGIC MAXGIFTS STRING,
# MAGIC MAXVECEST STRING,
# MAGIC MAXGIFTT STRING,
# MAGIC SENACU STRING,
# MAGIC PSTYV STRING,
# MAGIC ADDGRMID1 STRING,
# MAGIC ZSD_ZFLAGM STRING,
# MAGIC ZSD_FOBSEQ STRING,
# MAGIC ZSD_OFEVAL STRING,
# MAGIC ZSD_CSTOFE STRING,
# MAGIC ZSD_PRONRS4 STRING,
# MAGIC ZSD_FVTAWEB STRING,
# MAGIC ZSD_TCODE STRING,
# MAGIC ZSD_INDMP STRING,
# MAGIC ZSD_CCUPON STRING,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_sx/bronze/zsd_tprobo00'
# MAGIC     
