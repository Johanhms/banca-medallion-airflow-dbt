from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'Johan',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'banca_poblado_sucio_bronze',
    default_args=default_args,
    description='Poblado de datos crudos y sucios en Capa Bronze para pruebas de dbt',
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Poblar Dimensión Clientes (Con problemas de RUT y mayúsculas/minúsculas)
    poblar_clientes = SQLExecuteQueryOperator(
        task_id='poblar_dim_clientes_sucio',
        conn_id='postgres_default',
        sql="""
            TRUNCATE TABLE capa_bronze.dim_clientes;
            INSERT INTO capa_bronze.dim_clientes (cliente_id, rut, nombre, segmento, comuna) VALUES
            (1, '12.345.678-9', 'JOHAN MECIA', 'Banca Preferente', 'Providencia'),
            (2, '15456789K', 'pedro perez gonzalez', 'banca personas', 'SANTIAGO'),
            (3, '18.999.888-k', ' Maria Loreto Silva ', 'BANCA PERSONAS', ' Las Condes '),
            (4, '22333444-5', 'CARLOS MUÑOZ M.', 'Inversiones', 'PROVIDENCIA'),
            (5, '9876543-2', 'LUIS FONSI', NULL, 'Ñuñoa');
        """,
    )

    # 2. Poblar Dimensión Productos (Con familias inconsistentes)
    poblar_productos = SQLExecuteQueryOperator(
        task_id='poblar_dim_productos_sucio',
        conn_id='postgres_default',
        sql="""
            TRUNCATE TABLE capa_bronze.dim_productos;
            INSERT INTO capa_bronze.dim_productos (producto_id, codigo_producto, nombre_producto, familia_producto) VALUES
            (10, 'CTA_CTE_01', 'Cuenta Corriente Moneda Nacional', 'CUENTAS'),
            (20, 'TARJ_CRED_GT', 'Tarjeta Credito Visa GT', 'tarjetas'),
            (30, 'CRED_CONS_02', 'Credito Consumo Flexible', 'CREDITOS'),
            (40, 'CTA_CTE_USD', 'CUENTA CORRIENTE DOLARES', 'Cuentas'),
            (50, 'SEGURO_AUTO', 'Seguro Automotriz Todo Riesgo', 'SEGUROS');
        """,
    )

    # 3. Poblar Dimensión Ejecutivos (Con espacios y nulos)
    poblar_ejecutivos = SQLExecuteQueryOperator(
        task_id='poblar_dim_ejecutivos_sucio',
        conn_id='postgres_default',
        sql="""
            TRUNCATE TABLE capa_bronze.dim_ejecutivos;
            INSERT INTO capa_bronze.dim_ejecutivos (ejecutivo_id, codigo_empleado, nombre_ejecutivo, sucursal) VALUES
            (100, 'EMP_990', 'ANDRES BELLO', 'Casa Matriz'),
            (200, 'EMP_882', 'diego portales', 'providencia'),
            (300, 'EMP_112', '  Michelle B.  ', 'SANTIAGO CENTRO'),
            (400, 'EMP_001', 'SIN EJECUTIVO ASIGNADO', 'NO APLICA');
        """,
    )

    # 4. Poblar Tabla de Hechos Ventas (Con montos en cero, negativos o huérfanos)
    poblar_ventas = SQLExecuteQueryOperator(
        task_id='poblar_fact_ventas_sucio',
        conn_id='postgres_default',
        sql="""
            TRUNCATE TABLE capa_bronze.fact_ventas_bancarias;
            INSERT INTO capa_bronze.fact_ventas_bancarias (fecha_id, cliente_id, producto_id, ejecutivo_id, monto_apertura, tasa_interes) VALUES
            (20260528, 1, 10, 100, 500000.00, 0.00),
            (20260528, 2, 20, 200, 0.00, 2.45),
            (20260527, 3, 30, 300, 4500000.00, 1.15),
            (20260527, 4, 40, 100, -1500.50, 0.00), -- Monto negativo erróneo
            (99991231, 5, 50, 400, 120000.00, 0.99),  -- Fecha mala/outlier
            (20260528, 99, 10, 100, 300000.00, 1.20); -- ID Cliente 99 no existe (Error de integridad)
        """,
    )

    # Flujo de ejecución en paralelo para poblar las tablas
    [poblar_clientes, poblar_productos, poblar_ejecutivos] >> poblar_ventas