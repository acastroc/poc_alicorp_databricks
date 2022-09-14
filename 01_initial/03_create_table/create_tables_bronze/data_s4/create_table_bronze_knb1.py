# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.knb1 ;
# MAGIC create table bronze.knb1
# MAGIC (
# MAGIC MANDT string,
# MAGIC KUNNR string,
# MAGIC BUKRS string,
# MAGIC PERNR string,
# MAGIC KNB1_EEW_CC string,
# MAGIC ERDAT string,
# MAGIC ERNAM string,
# MAGIC SPERR string,
# MAGIC LOEVM string,
# MAGIC ZUAWA string,
# MAGIC BUSAB string,
# MAGIC AKONT string,
# MAGIC BEGRU string,
# MAGIC KNRZE string,
# MAGIC KNRZB string,
# MAGIC ZAMIM string,
# MAGIC ZAMIV string,
# MAGIC ZAMIR string,
# MAGIC ZAMIB string,
# MAGIC ZAMIO string,
# MAGIC ZWELS string,
# MAGIC XVERR string,
# MAGIC ZAHLS string,
# MAGIC ZTERM string,
# MAGIC WAKON string,
# MAGIC VZSKZ string,
# MAGIC ZINDT string,
# MAGIC ZINRT string,
# MAGIC EIKTO string,
# MAGIC ZSABE string,
# MAGIC KVERM string,
# MAGIC FDGRV string,
# MAGIC VRBKZ string,
# MAGIC VLIBB string,
# MAGIC VRSZL string,
# MAGIC VRSPR string,
# MAGIC VRSNR string,
# MAGIC VERDT string,
# MAGIC PERKZ string,
# MAGIC XDEZV string,
# MAGIC XAUSZ string,
# MAGIC WEBTR string,
# MAGIC REMIT string,
# MAGIC DATLZ string,
# MAGIC XZVER string,
# MAGIC TOGRU string,
# MAGIC KULTG string,
# MAGIC HBKID string,
# MAGIC XPORE string,
# MAGIC BLNKZ string,
# MAGIC ALTKN string,
# MAGIC ZGRUP string,
# MAGIC URLID string,
# MAGIC MGRUP string,
# MAGIC LOCKB string,
# MAGIC UZAWE string,
# MAGIC EKVBD string,
# MAGIC SREGL string,
# MAGIC XEDIP string,
# MAGIC FRGRP string,
# MAGIC VRSDG string,
# MAGIC TLFXS string,
# MAGIC INTAD string,
# MAGIC XKNZB string,
# MAGIC GUZTE string,
# MAGIC GRICD string,
# MAGIC GRIDT string,
# MAGIC WBRSL string,
# MAGIC CONFS string,
# MAGIC UPDAT string,
# MAGIC UPTIM string,
# MAGIC NODEL string,
# MAGIC TLFNS string,
# MAGIC CESSION_KZ string,
# MAGIC AVSND string,
# MAGIC AD_HASH string,
# MAGIC QLAND string,
# MAGIC CVP_XBLCK_B string,
# MAGIC CIIUCODE string,
# MAGIC GMVKZD string,
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/knb1'
# MAGIC     
