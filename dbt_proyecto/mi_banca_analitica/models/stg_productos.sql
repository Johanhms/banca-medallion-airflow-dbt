{{ config(materialized='table') }}

WITH productos_raw AS (
    SELECT 
        producto_id,
        codigo_producto,
        nombre_producto,
        familia_producto
    FROM {{ source('capa_bronze', 'dim_productos') }}
)

SELECT
    producto_id,
    TRIM(codigo_producto) AS codigo_producto,
    UPPER(TRIM(nombre_producto)) AS nombre_producto,
    UPPER(TRIM(familia_producto)) AS familia_producto
FROM productos_raw