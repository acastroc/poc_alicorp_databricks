-- Databricks notebook source
create table datagov.maestro_recursos_tms(
  rownum int,
  cod_sociedad string,
  cod_medio_transporte string,
  propietario string,
  flag_bloqueo_proveedor string,
  placa string,
  clase_vehiculo string ,
  flag_clase_vehiculo int,
  capacidad_masa decimal(15,4),
  unidad_medida_masa string,
  capacidad_masa_ton decimal(15,4),
  capacidad_min decimal(10,1),
  capacidad_max decimal(10,1),
  capacidad_volumen decimal(15,4),
  unidad_medida_volumen string,
  capacidad_agregada decimal(15,4),
  unidad_medida_agregada string,
  conductor string,
  tipo_agrupacion_recurso string,
  atributo_agrupacion_recurso string,
  dbkey string
)
using delta
LOCATION '/mnt/data_governance/fletes/maestro_recursos_tms'

-- COMMAND ----------

with base_maestro_recursos as (  
SELECT 
		ROW_NUMBER()OVER(ORDER BY codigo_recurso) AS rownum,
		'PE11' AS cod_sociedad,
		R.codigo_recurso,
		R.cod_medio_transporte,
		R.tipo_medio_transporte,
		R.propietario,
		P.sperr AS flag_bloqueo_proveedor,
		R.placa,
		R.clase_vehiculo,
		CASE WHEN L.clase_vehiculo IS NOT NULL THEN 1 ELSE 0 END AS flag_clase_vehiculo,
		R.capacidad_masa,
		R.unidad_medida_masa,
		CASE WHEN R.unidad_medida_masa='KG' THEN ROUND(R.capacidad_masa/1000,4) ELSE R.capacidad_masa END AS capacidad_masa_ton,
		C.capacidad_min,
		C.capacidad_max,
		R.capacidad_volumen,
		R.unidad_medida_volumen,
		R.capacidad_agregada,
		R.unidad_medida_agregada,
		R.conductor,
		CASE WHEN D.brevete IS NOT NULL THEN 1 ELSE 0 END AS flag_conductor,
		R.tipo_agrupacion_recurso ,
		R.atributo_agrupacion_recurso ,
		R.dbkey 
	FROM (
		select
			recurso AS codigo_recurso,
			medio_transporte AS cod_medio_transporte,
			CASE 
				WHEN medio_transporte IN ('ZPECT2', 'ZPECT3', 'ZPECT4') THEN 'Tracto'
				WHEN medio_transporte IN ('ZPEC01', 'ZPEC02', 'ZPEC03', 'ZPEC04') THEN 'Compacto'
				WHEN medio_transporte IN ('ZPEFS2', 'ZPEFS3') THEN 'Carreta'
				ELSE 'Otro'
			END AS tipo_medio_transporte,
			propietario,
			placa,
			clase_vehiculo,
			CASE 
				WHEN dimension_1='MASS' THEN capacidad_1
				WHEN dimension_2='MASS' THEN capacidad_2
				WHEN dimension_3='MASS' THEN capacidad_3
			END AS capacidad_masa,
			CASE 
				WHEN dimension_1='MASS' THEN unidad_medida_1
				WHEN dimension_2='MASS' THEN unidad_medida_2
				WHEN dimension_3='MASS' THEN unidad_medida_3
			END AS unidad_medida_masa,
			CASE 
				WHEN dimension_1='VOLUME' THEN capacidad_1
				WHEN dimension_2='VOLUME' THEN capacidad_2
				WHEN dimension_3='VOLUME' THEN capacidad_3
			END AS capacidad_volumen,
			CASE 
				WHEN dimension_1='VOLUME' THEN unidad_medida_1
				WHEN dimension_2='VOLUME' THEN unidad_medida_2
				WHEN dimension_3='VOLUME' THEN unidad_medida_3
			END AS unidad_medida_volumen,
			CASE 
				WHEN dimension_1='AAAADL' THEN capacidad_1
				WHEN dimension_2='AAAADL' THEN capacidad_2
				WHEN dimension_3='AAAADL' THEN capacidad_3
			END AS capacidad_agregada,
			CASE 
				WHEN dimension_1='AAAADL' THEN unidad_medida_1
				WHEN dimension_2='AAAADL' THEN unidad_medida_2
				WHEN dimension_3='AAAADL' THEN unidad_medida_3
			END AS unidad_medida_agregada,
			conductor_estandar AS conductor,
			tipo_agrupacion_recurso,
			atributo_agrupacion_recurso,
			recurso AS dbkey
		from datagov.fletes_maestro_recursos_raw
		) R
		LEFT JOIN datagov.p_capacidad_vehiculo_tms C
			ON R.tipo_medio_transporte=C.tipo_medio_transporte
		LEFT JOIN datagov.p_clase_vehiculo_tms L
			ON R.tipo_medio_transporte=L.tipo_medio_transporte AND R.clase_vehiculo=L.clase_vehiculo
		LEFT JOIN bronze.lfa1 P
			ON R.propietario=P.lifnr
		LEFT JOIN datagov.maestro_conductores_tms D
			ON R.conductor=D.brevete
  
 )
insert into datagov.maestro_recursos_tms
select 
  *
from base_maestro_recursos

-- COMMAND ----------

with base_maestro_recursos as (  
SELECT 
		ROW_NUMBER()OVER(ORDER BY codigo_recurso) AS rownum,
		'PE11' AS cod_sociedad,
		R.codigo_recurso,
		R.cod_medio_transporte,
		R.tipo_medio_transporte,
		R.propietario,
		P.sperr AS flag_bloqueo_proveedor,
		R.placa,
		R.clase_vehiculo,
		CASE WHEN L.CLASE_VEHICULO IS NOT NULL THEN 1 ELSE 0 END AS flag_clase_vehiculo,
		R.capacidad_masa,
		R.unidad_medida_masa,
		CASE WHEN R.unidad_medida_masa='KG' THEN ROUND(R.capacidad_masa/1000,4) ELSE R.capacidad_masa END AS capacidad_masa_ton,
		C.capacidad_min,
		C.capacidad_max,
		R.capacidad_volumen,
		R.unidad_medida_volumen,
		R.capacidad_agregada,
		R.unidad_medida_agregada,
		R.conductor,
		CASE WHEN D.brevete IS NOT NULL THEN 1 ELSE 0 END AS flag_conductor,
		R.tipo_agrupacion_recurso,
		R.atributo_agrupacion_recurso,
		R.dbkey
	FROM (
            select
                recurso AS codigo_recurso,
                medio_transporte AS cod_medio_transporte,
                CASE 
                    WHEN medio_transporte IN ('ZPECT2', 'ZPECT3', 'ZPECT4') THEN 'Tracto'
                    WHEN medio_transporte IN ('ZPEC01', 'ZPEC02', 'ZPEC03', 'ZPEC04') THEN 'Compacto'
                    WHEN medio_transporte IN ('ZPEFS2', 'ZPEFS3') THEN 'Carreta'
                    ELSE 'Otro'
                END AS tipo_medio_transporte,
                propietario,
                placa,
                clase_vehiculo,
                CASE 
                    WHEN dimension_1='MASS' THEN capacidad_1
                    WHEN dimension_2='MASS' THEN capacidad_2
                    WHEN dimension_3='MASS' THEN capacidad_3
                END AS capacidad_masa,
                CASE 
                    WHEN dimension_1='MASS' THEN unidad_medida_1
                    WHEN dimension_2='MASS' THEN unidad_medida_2
                    WHEN dimension_3='MASS' THEN unidad_medida_3
                END AS unidad_medida_masa,
                CASE 
                    WHEN dimension_1='VOLUME' THEN capacidad_1
                    WHEN dimension_2='VOLUME' THEN capacidad_2
                    WHEN dimension_3='VOLUME' THEN capacidad_3
                END AS capacidad_volumen,
                CASE 
                    WHEN dimension_1='VOLUME' THEN unidad_medida_1
                    WHEN dimension_2='VOLUME' THEN unidad_medida_2
                    WHEN dimension_3='VOLUME' THEN unidad_medida_3
                END AS unidad_medida_volumen,
                CASE 
                    WHEN dimension_1='AAAADL' THEN capacidad_1
                    WHEN dimension_2='AAAADL' THEN capacidad_2
                    WHEN dimension_3='AAAADL' THEN capacidad_3
                END AS capacidad_agregada,
                CASE 
                    WHEN dimension_1='AAAADL' THEN unidad_medida_1
                    WHEN dimension_2='AAAADL' THEN unidad_medida_2
                    WHEN dimension_3='AAAADL' THEN unidad_medida_3
                END AS unidad_medida_agregada,
                conductor_estandar AS conductor,
                tipo_agrupacion_recurso,
                atributo_agrupacion_recurso,
                recurso AS dbkey
            from datagov.fletes_maestro_recursos_raw limit 100
          ) R
            LEFT JOIN datagov.p_capacidad_vehiculo_tms C
                ON R.tipo_medio_transporte=C.tipo_medio_transporte
            LEFT JOIN datagov.p_clase_vehiculo_tms L
                ON R.tipo_medio_transporte=L.tipo_medio_transporte AND R.clase_vehiculo=L.clase_vehiculo
            LEFT JOIN bronze.lfa1 P
                ON R.propietario=P.lifnr
            LEFT JOIN datagov.maestro_conductores_tms D
                ON R.conductor=D.brevete
  
 )
 
 select * from base_maestro_recursos limit 100

-- COMMAND ----------

SELECT 
		ROW_NUMBER()OVER(ORDER BY codigo_recurso) AS rownum,
		'PE11' AS cod_sociedad,
		R.codigo_recurso,
		R.cod_medio_transporte,
		R.tipo_medio_transporte,
		R.propietario,
		P.sperr AS flag_bloqueo_proveedor,
		R.placa,
		R.clase_vehiculo,
		CASE WHEN L.CLASE_VEHICULO IS NOT NULL THEN 1 ELSE 0 END AS flag_clase_vehiculo,
		R.capacidad_masa,
		R.unidad_medida_masa,
		CASE WHEN R.unidad_medida_masa='KG' THEN ROUND(R.capacidad_masa/1000,4) ELSE R.capacidad_masa END AS capacidad_masa_ton,
		C.capacidad_min,
		C.capacidad_max,
		R.capacidad_volumen,
		R.unidad_medida_volumen,
		R.capacidad_agregada,
		R.unidad_medida_agregada,
		R.conductor,
		CASE WHEN D.brevete IS NOT NULL THEN 1 ELSE 0 END AS flag_conductor,
		R.tipo_agrupacion_recurso,
		R.atributo_agrupacion_recurso,
		R.dbkey
	FROM (
            select
                recurso AS codigo_recurso,
                medio_transporte AS cod_medio_transporte,
                CASE 
                    WHEN medio_transporte IN ('ZPECT2', 'ZPECT3', 'ZPECT4') THEN 'Tracto'
                    WHEN medio_transporte IN ('ZPEC01', 'ZPEC02', 'ZPEC03', 'ZPEC04') THEN 'Compacto'
                    WHEN medio_transporte IN ('ZPEFS2', 'ZPEFS3') THEN 'Carreta'
                    ELSE 'Otro'
                END AS tipo_medio_transporte,
                propietario,
                placa,
                clase_vehiculo,
                CASE 
                    WHEN dimension_1='MASS' THEN capacidad_1
                    WHEN dimension_2='MASS' THEN capacidad_2
                    WHEN dimension_3='MASS' THEN capacidad_3
                END AS capacidad_masa,
                CASE 
                    WHEN dimension_1='MASS' THEN unidad_medida_1
                    WHEN dimension_2='MASS' THEN unidad_medida_2
                    WHEN dimension_3='MASS' THEN unidad_medida_3
                END AS unidad_medida_masa,
                CASE 
                    WHEN dimension_1='VOLUME' THEN capacidad_1
                    WHEN dimension_2='VOLUME' THEN capacidad_2
                    WHEN dimension_3='VOLUME' THEN capacidad_3
                END AS capacidad_volumen,
                CASE 
                    WHEN dimension_1='VOLUME' THEN unidad_medida_1
                    WHEN dimension_2='VOLUME' THEN unidad_medida_2
                    WHEN dimension_3='VOLUME' THEN unidad_medida_3
                END AS unidad_medida_volumen,
                CASE 
                    WHEN dimension_1='AAAADL' THEN capacidad_1
                    WHEN dimension_2='AAAADL' THEN capacidad_2
                    WHEN dimension_3='AAAADL' THEN capacidad_3
                END AS capacidad_agregada,
                CASE 
                    WHEN dimension_1='AAAADL' THEN unidad_medida_1
                    WHEN dimension_2='AAAADL' THEN unidad_medida_2
                    WHEN dimension_3='AAAADL' THEN unidad_medida_3
                END AS unidad_medida_agregada,
                conductor_estandar AS conductor,
                tipo_agrupacion_recurso,
                atributo_agrupacion_recurso,
                recurso AS DBKEY
            from datagov.fletes_maestro_recursos_raw limit 100
          ) R
            LEFT JOIN datagov.p_capacidad_vehiculo_tms C
                ON R.tipo_medio_transporte=C.tipo_medio_transporte
            LEFT JOIN datagov.p_clase_vehiculo_tms L
                ON R.tipo_medio_transporte=L.tipo_medio_transporte AND R.clase_vehiculo=L.clase_vehiculo
            LEFT JOIN bronze.lfa1 P
                ON R.propietario=P.lifnr
            LEFT JOIN datagov.maestro_conductores_tms D
                ON R.conductor=D.brevete
  
