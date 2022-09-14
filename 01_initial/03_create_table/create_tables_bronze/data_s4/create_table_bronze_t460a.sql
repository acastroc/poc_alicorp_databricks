-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists bronze.t460a ;
-- MAGIC create table bronze.t460a
-- MAGIC (
-- MAGIC mandt string ,
-- MAGIC werks string ,
-- MAGIC sobsl string ,
-- MAGIC beskz string ,
-- MAGIC sobes string ,
-- MAGIC wrk02 string ,
-- MAGIC clcor string ,
-- MAGIC dumps string ,
-- MAGIC rewfg string ,
-- MAGIC rewrk string ,
-- MAGIC dirpr string ,
-- MAGIC umldb string ,
-- MAGIC addin string ,
-- MAGIC mlscr string ,
-- MAGIC create_at timestamp ,
-- MAGIC origin_file string ,
-- MAGIC year_month string 
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (YEAR_MONTH)
-- MAGIC LOCATION '/mnt/data_s4/bronze/t460a'
-- MAGIC     
