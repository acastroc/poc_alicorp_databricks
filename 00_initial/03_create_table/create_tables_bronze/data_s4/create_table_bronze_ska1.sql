-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists bronze.ska1 ;
-- MAGIC create table bronze.ska1
-- MAGIC (
-- MAGIC mandt string,
-- MAGIC ktopl string,
-- MAGIC saknr string,
-- MAGIC xbilk string,
-- MAGIC sakan string,
-- MAGIC bilkt string,
-- MAGIC erdat string,
-- MAGIC ernam string,
-- MAGIC gvtyp string,
-- MAGIC ktoks string,
-- MAGIC mustr string,
-- MAGIC vbund string,
-- MAGIC xloev string,
-- MAGIC xspea string,
-- MAGIC xspeb string,
-- MAGIC xspep string,
-- MAGIC mcod1 string,
-- MAGIC func_area string,
-- MAGIC glaccount_type string,
-- MAGIC last_changed_ts string,
-- MAGIC create_at timestamp ,
-- MAGIC origin_file string ,
-- MAGIC year_month_day string 
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (year_month_day)
-- MAGIC LOCATION '/mnt/data_s4/bronze/ska1'