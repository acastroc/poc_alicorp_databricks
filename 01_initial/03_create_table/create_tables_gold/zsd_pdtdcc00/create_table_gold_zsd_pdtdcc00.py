# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists gold.zsd_pdtdcc00 ;
# MAGIC create table gold.zsd_pdtdcc00
# MAGIC (
# MAGIC MANDT STRING
# MAGIC ,ZSD_NDFACT STRING
# MAGIC ,ZSD_NPOSIC STRING
# MAGIC ,ZSD_CCLPOS STRING
# MAGIC ,ZSD_SPOSIC STRING
# MAGIC ,ZSD_CMOTIV STRING
# MAGIC ,ZSD_CMATER STRING
# MAGIC ,ZSD_QMATER INT
# MAGIC ,ZSD_CUNMED STRING
# MAGIC ,ZSD_QLIQVT STRING
# MAGIC ,ZSD_QLIQBA STRING
# MAGIC ,ZSD_QDEBAS STRING
# MAGIC ,ZSD_RPERCE STRING
# MAGIC ,ZSD_IUNITA DECIMAL(22,3)
# MAGIC ,ZSD_IDCTUN STRING
# MAGIC ,ZSD_IRECUN STRING
# MAGIC ,ZSD_IBRUTO DECIMAL(22,3)
# MAGIC ,ZSD_IDESCT DECIMAL(22,3)
# MAGIC ,ZSD_IRECAR STRING
# MAGIC ,ZSD_IIMPUE DECIMAL(22,3)
# MAGIC ,ZSD_INETO DECIMAL(22,3)
# MAGIC ,ZSD_INEACT DECIMAL(22,3)
# MAGIC ,ZSD_TTAX1 STRING
# MAGIC ,ZSD_VTAX1 STRING
# MAGIC ,ZSD_TTAX2 STRING
# MAGIC ,ZSD_VTAX2 DECIMAL(22,3)
# MAGIC ,ZSD_TTAX3 STRING
# MAGIC ,ZSD_VTAX3 STRING
# MAGIC ,ZSD_TTAX4 STRING
# MAGIC ,ZSD_VTAX4 STRING
# MAGIC ,ZSD_TTAX5 STRING
# MAGIC ,ZSD_VTAX5 STRING
# MAGIC ,ZSD_TTAX6 STRING
# MAGIC ,ZSD_VTAX6 STRING
# MAGIC ,ZSD_TTAX7 STRING
# MAGIC ,ZSD_VTAX7 STRING
# MAGIC ,ZSD_TTAX8 STRING
# MAGIC ,ZSD_VTAX8 STRING
# MAGIC ,ZSD_TTAX9 STRING
# MAGIC ,ZSD_VTAX9 STRING
# MAGIC ,ZSD_CMONED STRING
# MAGIC ,ZSD_IUNIML DECIMAL(22,3)
# MAGIC ,ZSD_IBRUML DECIMAL(22,3)
# MAGIC ,ZSD_IDSCML DECIMAL(22,3)
# MAGIC ,ZSD_IRECML STRING
# MAGIC ,ZSD_IIMPML DECIMAL(22,3)
# MAGIC ,ZSD_INETML DECIMAL(22,3)
# MAGIC ,ZSD_CMONLO STRING
# MAGIC ,ZSD_PBRUTO DECIMAL(22,3)
# MAGIC ,ZSD_PNETO DECIMAL(22,3)
# MAGIC ,ZSD_CUNMPE STRING
# MAGIC ,ZSD_VENTTO DECIMAL(22,3)
# MAGIC ,ZSD_CUNMVO STRING
# MAGIC ,ZSD_NPOSOR STRING
# MAGIC ,ZSD_CPROMO STRING
# MAGIC ,ZSD_CENTRE STRING
# MAGIC ,ZSD_CNEGOC STRING
# MAGIC ,ZSD_LPROMT STRING
# MAGIC ,ZSD_CPEDTG STRING
# MAGIC ,ZSD_CPEDID STRING
# MAGIC ,ZSD_IDESPD STRING
# MAGIC ,ZSD_QMATAV STRING
# MAGIC ,ZSD_CUNMAV STRING
# MAGIC ,ZSD_FPRDSG STRING
# MAGIC ,ZSD_IMARGEN DECIMAL(22,3)
# MAGIC ,ZSD_DEPOALIC DECIMAL(22,3)
# MAGIC ,ZSD_IBRUFE STRING
# MAGIC ,ZSD_IDSCFE STRING
# MAGIC ,ZSD_IRECFE STRING
# MAGIC ,ZSD_IIMPFE STRING
# MAGIC ,ZSD_INETFE STRING
# MAGIC ,ZSD_INACFE STRING
# MAGIC 
# MAGIC ,CREATE_AT timestamp
# MAGIC ,YEAR_MONTH_DAY string
# MAGIC ,ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_sx/gold/zsd_pdtdcc00'
# MAGIC     
