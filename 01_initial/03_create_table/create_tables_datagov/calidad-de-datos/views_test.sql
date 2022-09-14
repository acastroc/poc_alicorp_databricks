-- Databricks notebook source
drop view conductores_test;

-- COMMAND ----------

    CREATE OR REPLACE VIEW datagov.conductores_test
    (conductor, apellido, nombre)
    AS SELECT conductor, apellido, nombre
         FROM uf.fletes_maestro_conductores
        WHERE nombre='JUAN' ;
  
   

-- COMMAND ----------

select * from  uf.fletes_maestro_conductores limit 10;

-- COMMAND ----------

select count(*) from conductores_test;

-- COMMAND ----------

select * from conductores_test;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import pyspark
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql import Row
-- MAGIC appName= "hive_pyspark"
-- MAGIC master= "local"
-- MAGIC 
-- MAGIC 
-- MAGIC spark = SparkSession.builder \
-- MAGIC 	.master(master).appName(appName).enableHiveSupport().getOrCreate()
-- MAGIC 
-- MAGIC df=spark.sql("show databases")
-- MAGIC df.show()
-- MAGIC 
-- MAGIC tables = spark.sql("show tables").show();
-- MAGIC 
-- MAGIC df1=spark.sql("select * from conductores_test ")
-- MAGIC df1.show()
-- MAGIC 
-- MAGIC df1.write.csv("/mnt/data_governance/calidad-de-datos/test_export")
-- MAGIC //df1.to_excel()

-- COMMAND ----------

df1=spark.sql("select * from uf.fletes_maestro_conductores limit 5 ")
df1.show()

-- COMMAND ----------

SHOW CREATE TABLE datagov.conductores_test

-- COMMAND ----------


