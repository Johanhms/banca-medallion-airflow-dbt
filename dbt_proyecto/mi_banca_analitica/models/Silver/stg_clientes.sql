{{ config(materialized='table') }}

WITH clientes_raw AS (
    SELECT 
        cliente_id,
        rut,
        nombre,
        segmento,
        comuna
    FROM {{ source('capa_bronze', 'dim_clientes') }} 
)

SELECT
    cliente_id,
    -- Limpieza de RUT: Quitamos puntos, pasamos a mayúsculas y eliminamos espacios
    UPPER(REPLACE(REPLACE(TRIM(rut), '.', ''), ' ', '')) AS rut_limpio,
    -- Limpieza de Nombre: Quitamos espacios extras y convertimos a mayúsculas para estandarizar
    UPPER(TRIM(nombre)) AS nombre_limpio,
    -- Manejo de Nulos en Segmento
    COALESCE(TRIM(segmento), 'SIN SEGMENTO') AS segmento,
    -- Limpieza de Comuna
    UPPER(TRIM(comuna)) AS comuna
FROM clientes_raw