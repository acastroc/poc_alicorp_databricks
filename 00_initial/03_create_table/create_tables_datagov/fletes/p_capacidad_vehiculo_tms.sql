-- Databricks notebook source
create table datagov.p_capacidad_vehiculo_tms(
  tipo_medio_transporte string,
  capacidad_min decimal(10,1),
  capacidad_max decimal(10,1)
)
using delta
LOCATION '/mnt/data_governance/fletes/p_capacidad_vehiculo_tms'

-- COMMAND ----------


INSERT INTO datagov.p_capacidad_vehiculo_tms VALUES ('Carreta',20,35);
INSERT INTO datagov.p_capacidad_vehiculo_tms VALUES ('Compacto',0.8,20);
