{{ config(materialized='table') }}

WITH ventas_raw AS (
    SELECT 
        venta_id,
        fecha_id,
        cliente_id,
        producto_id,
        ejecutivo_id,
        monto_apertura,
        tasa_interes
    FROM {{ source('capa_bronze', 'fact_ventas_bancarias') }}
)

SELECT
    venta_id,
    fecha_id,
    cliente_id,
    producto_id,
    ejecutivo_id,
    -- Regla de negocio: Si el monto es negativo por error, lo normalizamos a 0 o lo filtramos
    CASE 
        WHEN monto_apertura < 0 THEN 0.00
        ELSE monto_apertura 
    END AS monto_apertura,
    COALESCE(tasa_interes, 0.00) AS tasa_interes
FROM ventas_raw
-- Filtramos transacciones con fechas outliers de error si es necesario, por ahora dejamos pasar para verlas