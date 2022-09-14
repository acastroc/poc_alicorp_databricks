-- Databricks notebook source
create table datagov.maestro_geocoordenadas_tms(
    rownum int,
    cod_sociedad string,
    cliente string,
    nombre string,
    xpos decimal(15,4),
    ypos decimal(15,4),
    pais_geocoordenda string
    dbkey
)
using delta
LOCATION '/mnt/data_governance/fletes/maestro_geocoordenadas_tms'

-- COMMAND ----------

with base_geocoordenadas  as (  
  select 
		ROW_NUMBER()OVER(ORDER BY G.CLIENTE) AS rownum,
		'PE11' AS cod_sociedad,
		G.cliente,
		G.nombre,
		G.xpos,
		G.ypos, 
		P.PAIS AS pais_geocoordenada,
		COALESCE(cliente,'') || COALESCE(nombre,'') AS DBKEY
	from datagov.fletes_geocoordenadas_raw G
	LEFT JOIN P_GEOCOORDENADAS P
		ON G.XPOS=P.XPOS AND G.YPOS=P.YPOS
  
 )

insert into datagov.maestro_geocoordenadas_tms 
select 
  *
from base_geocoordenadas
