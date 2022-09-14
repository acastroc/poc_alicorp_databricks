-- Databricks notebook source
drop view conductores_test;

-- COMMAND ----------

CREATE OR REPLACE VIEW datagov.vw_observados_tarifas_transporte
( cod_sociedad,
  cod_proveedor,
  razon_social,
  cod_zona_expedicion,
  des_zona_expedicion,
  cod_zona_destino,
  des_zona_destino,
  tabla_tarifas,
  tipo_tarifa,
  tipo_unidad,
  capacidad,
  grupo_porte_prod,
  tarifa,
  inicio_validez,
  fin_validez,
  num_controles_error,
  detalle_controles_error)
as(
WITH regs_obs AS (
            SELECT
                D.rownum,
                substr(D.id_control,9,8) AS ID_CONTROL
            FROM datagov.registros_observados_stg D
            JOIN datagov.maestro_controles_prd M
                ON D.id_control=M.id_control
            WHERE M.aprobado=1
            AND M.frente='Fletes y Acarreos'
            AND M.subdominio='Tarifas'
            AND D.fecha_proceso=CURRENT_DATE
),
base_resumen AS (
    SELECT
        rownum,
        COUNT(id_control) AS num_controles_error,
        --STRING_AGG(id_control,',' ORDER BY id_control) AS detalle_controles_error
        array_join(collect_set(id_control), ',') AS detalle_controles_error
    FROM regs_obs
    GROUP BY rownum
)
SELECT
    A.cod_sociedad,
    A.cod_proveedor,
    A.razon_social,
    A.cod_zona_expedicion,
    A.des_zona_expedicion,
    A.cod_zona_destino,
    A.des_zona_destino,
    A.tabla_tarifas,
    A.tipo_tarifa,
    A.tipo_unidad,
    A.capacidad,
    A.grupo_porte_prod,
    A.tarifa,
    A.inicio_validez,
    A.fin_validez,
    R.num_controles_error,
    R.detalle_controles_error
FROM datagov.maestro_tarifas_transporte_tms A
JOIN base_resumen R
    ON A.rownum=R.rownum
)

  

-- COMMAND ----------


