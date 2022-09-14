# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.zsd_trango00 ;
# MAGIC create table bronze.zsd_trango00
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC ZSD_CENTOR STRING,
# MAGIC ZSDS_CPROMO STRING,
# MAGIC ZSD_NRANGO STRING,
# MAGIC ZSD_NCORRE STRING,
# MAGIC ZSD_QDESMI STRING,
# MAGIC ZSD_QHASMX STRING,
# MAGIC ZSD_QXCADA STRING,
# MAGIC ZSD_CUNMRA STRING,
# MAGIC ZSD_CMATER STRING,
# MAGIC ZSD_CUNMRE STRING,
# MAGIC ZSD_QOBSEQ STRING,
# MAGIC ZSD_RDESCU STRING,
# MAGIC ZSD_DUSCRE STRING,
# MAGIC ZSD_FCREAC STRING,
# MAGIC ZSD_DUSMOD STRING,
# MAGIC ZSD_FMODIF STRING,
# MAGIC ZSD_SIGNO STRING,
# MAGIC MAXBONIF STRING,
# MAGIC PRCTR STRING,
# MAGIC ZSD_ZFLAGM STRING,
# MAGIC ZSD_MAXPED STRING,
# MAGIC ZSD_PBASIN STRING,
# MAGIC ZSD_PBASFN STRING,
# MAGIC ZSD_CREVIN STRING,
# MAGIC ZSD_CREVFI STRING,
# MAGIC ZSD_UNIBAS STRING,
# MAGIC ZSD_VALABS STRING,
# MAGIC ZSD_DESCT STRING,
# MAGIC ZSD_UNIDES STRING,
# MAGIC ZSD_DESCRI STRING,
# MAGIC ZSD_FACTOV STRING,
# MAGIC ZSD_DSCCUP STRING,
# MAGIC ZSD_MNTOMX STRING,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_sx/bronze/zsd_trango00'
# MAGIC     