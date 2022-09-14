-- Databricks notebook source
create table datagov.p_clase_vehiculo_tms(
  tipo_medio_transporte string,
  clase_vehiculo string
)
using delta
LOCATION '/mnt/data_governance/fletes/p_clase_vehiculo_tms'

-- COMMAND ----------


insert into datagov.p_clase_vehiculo_tms VALUES ('Tracto','T2');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Tracto','T3');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Tracto','T4');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Compacto','C1');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Compacto','C2');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Compacto','C3');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Carreta','S2');
INSERT INTO datagov.p_clase_vehiculo_tms VALUES ('Carreta','S3');

-- COMMAND ----------

select * from datagov.p_clase_vehiculo_tms;
