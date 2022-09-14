# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.zsd_msede00 ;
# MAGIC create table silver.zsd_msede00
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC ZSD_CSOCIE STRING,
# MAGIC ZSD_CSEDE STRING,
# MAGIC ZSD_CSAP STRING,
# MAGIC ZSD_CGEREG STRING,
# MAGIC ZSD_DNOMBR STRING,
# MAGIC ZSD_CGEZNA STRING,
# MAGIC ZSD_COFVTA STRING,
# MAGIC ZSD_CCENTR STRING,
# MAGIC ZSD_CCLIEN STRING,
# MAGIC ZSD_CREGIO STRING,
# MAGIC ZSD_LDIRNE STRING,
# MAGIC ZSD_LRUCNE STRING,
# MAGIC ZSD_LAPROB STRING,
# MAGIC ZSD_NMCXVE STRING,
# MAGIC ZSD_NDIAAJU STRING,
# MAGIC ZSD_FOCTCE STRING,
# MAGIC ZSD_LENTDIR STRING,
# MAGIC ZSD_LCCPO STRING,
# MAGIC ZSD_LOGRD STRING,
# MAGIC ZSD_CDIREC STRING,
# MAGIC ZSD_LPJVT STRING,
# MAGIC ZSD_FCREAC STRING,
# MAGIC ZSD_LDEUD STRING,
# MAGIC ZSD_LHRNS STRING,
# MAGIC ZSD_LPEXP STRING,
# MAGIC ZSD_CREGVE STRING,
# MAGIC ZSD_RCOPO STRING,
# MAGIC ZSD_IMPMR STRING,
# MAGIC ZSD_IMPME STRING,
# MAGIC ZSD_LNECO STRING,
# MAGIC ZSD_LPEIN STRING,
# MAGIC ZSD_LANEV STRING,
# MAGIC ZSD_LEPED STRING,
# MAGIC ZSD_NDMFEN STRING,
# MAGIC ZSD_NDMFEJIT STRING,
# MAGIC ZSD_NPRXML STRING,
# MAGIC ZSD_MPHNA STRING,
# MAGIC ZSD_LGRDFA STRING,
# MAGIC ZSD_LBMAE STRING,
# MAGIC ZSD_LVENRES STRING,
# MAGIC ZSD_CGRRE STRING,
# MAGIC ZSD_CREGI STRING,
# MAGIC ZSD_CZONA STRING,
# MAGIC ZSD_CTERAL STRING,
# MAGIC ZSD_LNCSUE STRING,
# MAGIC ZSD_CCREMP STRING,
# MAGIC ZSD_CSOCRE STRING,
# MAGIC ZSD_CSEDRE STRING,
# MAGIC ZSD_SSEDE STRING,
# MAGIC ZSD_FREFL STRING,
# MAGIC ZSD_LMIXF STRING,
# MAGIC ZSD_LPROM STRING,
# MAGIC ZSD_FBOCUM STRING,
# MAGIC ZSD_LCALT STRING,
# MAGIC ZSD_FCIER STRING,
# MAGIC ZSD_FPROS STRING,
# MAGIC ZSD_LEXIMP STRING,
# MAGIC ZSD_LINXCO STRING,
# MAGIC ZSD_FIMFEL STRING,
# MAGIC ZSD_LIMFED STRING,
# MAGIC ZSD_LTIPFE STRING,
# MAGIC ZSD_FINFED STRING,
# MAGIC ZSD_NTELEF STRING,
# MAGIC ZSD_CTPEXO STRING,
# MAGIC ZSD_NDIACON STRING,
# MAGIC ZSD_NUBL STRING,
# MAGIC ZSD_CCLIS4 STRING,
# MAGIC ZSD_LENVS4 STRING,
# MAGIC ZSD_FENVS4 STRING,
# MAGIC ZSD_BPROC STRING,
# MAGIC ZSD_BHORA STRING,
# MAGIC ZSD_GRPREC STRING,
# MAGIC 
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_sx/silver/zsd_msede00'
# MAGIC     
