-- Databricks notebook source
-- MAGIC %sql
-- MAGIC drop table if exists bronze.cepc ;
-- MAGIC create table bronze.cepc
-- MAGIC (
-- MAGIC mandt string,
-- MAGIC prctr string,
-- MAGIC datbi string,
-- MAGIC kokrs string,
-- MAGIC datab string,
-- MAGIC ersda string,
-- MAGIC usnam string,
-- MAGIC merkmal string,
-- MAGIC abtei string,
-- MAGIC verak string,
-- MAGIC verak_user string,
-- MAGIC waers string,
-- MAGIC nprctr string,
-- MAGIC land1 string,
-- MAGIC anred string,
-- MAGIC name1 string,
-- MAGIC name2 string,
-- MAGIC name3 string,
-- MAGIC name4 string,
-- MAGIC ort01 string,
-- MAGIC ort02 string,
-- MAGIC stras string,
-- MAGIC pfach string,
-- MAGIC pstlz string,
-- MAGIC pstl2 string,
-- MAGIC spras string,
-- MAGIC telbx string,
-- MAGIC telf1 string,
-- MAGIC telf2 string,
-- MAGIC telfx string,
-- MAGIC teltx string,
-- MAGIC telx1 string,
-- MAGIC datlt string,
-- MAGIC drnam string,
-- MAGIC khinr string,
-- MAGIC bukrs string,
-- MAGIC vname string,
-- MAGIC recid string,
-- MAGIC etype string,
-- MAGIC txjcd string,
-- MAGIC regio string,
-- MAGIC kvewe string,
-- MAGIC kappl string,
-- MAGIC kalsm string,
-- MAGIC logsystem string,
-- MAGIC lock_ind string,
-- MAGIC pca_template string,
-- MAGIC segment string,
-- MAGIC eew_cepc_ps_dummy string,
-- MAGIC create_at timestamp ,
-- MAGIC origin_file string ,
-- MAGIC year_month_day string 
-- MAGIC )
-- MAGIC using delta
-- MAGIC partitioned by (year_month_day)
-- MAGIC LOCATION '/mnt/data_s4/bronze/cepc'
