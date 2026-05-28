{{ config(materialized='table') }}

WITH ejecutivos_raw AS (
    SELECT 
        ejecutivo_id,
        codigo_empleado,
        nombre_ejecutivo,
        sucursal
    FROM {{ source('capa_bronze', 'dim_ejecutivos') }}
)

SELECT
    ejecutivo_id,
    TRIM(codigo_empleado) AS codigo_empleado,
    UPPER(TRIM(nombre_ejecutivo)) AS nombre_ejecutivo,
    UPPER(TRIM(sucursal)) AS sucursal
FROM ejecutivos_raw