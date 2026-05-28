{{ config(
    materialized='table',
    schema = 'capa_gold'
) }}

WITH ventas AS (
    SELECT * FROM {{ ref('stg_ventas') }}
),
clientes AS (
    SELECT * FROM {{ ref('stg_clientes') }}
),
productos AS (
    SELECT * FROM {{ ref('stg_productos') }}
),
ejecutivos AS (
    SELECT * FROM {{ ref('stg_ejecutivos') }}
)

SELECT
    v.venta_id,
    -- Control de fecha outlier (regla de negocio: si la fecha es errónea, se asigna un default)
    CASE 
        WHEN v.fecha_id = 99991231 THEN 19000101
        ELSE v.fecha_id 
    END AS fecha_key,
    v.cliente_id,
    c.rut_limpio AS cliente_rut,
    c.nombre_limpio AS cliente_nombre,
    c.segmento AS cliente_segmento,
    v.producto_id,
    p.nombre_producto,
    p.familia_producto,
    v.ejecutivo_id,
    e.nombre_ejecutivo,
    e.sucursal AS sucursal_venta,
    v.monto_apertura,
    v.tasa_interes
FROM ventas v
LEFT JOIN clientes c ON v.cliente_id = c.cliente_id
LEFT JOIN productos p ON v.producto_id = p.producto_id
LEFT JOIN ejecutivos e ON v.ejecutivo_id = e.ejecutivo_id