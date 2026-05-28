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
    'banca_modelo_estrella_bronze',
    default_args=default_args,
    description='Inicialización del Modelo Estrella Bancario - Capa Bronze',
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Crear el esquema Bronze
    crear_esquema = SQLExecuteQueryOperator(
        task_id='crear_esquema_bronze',
        conn_id='postgres_default',
        sql="CREATE SCHEMA IF NOT EXISTS capa_bronze;",
    )

    # 2. Crear Dimensión Clientes
    crear_dim_clientes = SQLExecuteQueryOperator(
        task_id='crear_table_dim_clientes',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS capa_bronze.dim_clientes (
                cliente_id INT PRIMARY KEY,
                rut VARCHAR(20),
                nombre VARCHAR(100),
                segmento VARCHAR(50),
                comuna VARCHAR(50)
            );
        """,
    )

    # 3. Crear Dimensión Productos
    crear_dim_productos = SQLExecuteQueryOperator(
        task_id='crear_table_dim_productos',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS capa_bronze.dim_productos (
                producto_id INT PRIMARY KEY,
                codigo_producto VARCHAR(20),
                nombre_producto VARCHAR(100),
                familia_producto VARCHAR(50)
            );
        """,
    )

    # 4. Crear Dimensión Ejecutivos
    crear_dim_ejecutivos = SQLExecuteQueryOperator(
        task_id='crear_table_dim_ejecutivos',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS capa_bronze.dim_ejecutivos (
                ejecutivo_id INT PRIMARY KEY,
                codigo_empleado VARCHAR(20),
                nombre_ejecutivo VARCHAR(100),
                sucursal VARCHAR(50)
            );
        """,
    )

    # 5. Crear Tabla de Hechos (Ventas)
    crear_fact_ventas = SQLExecuteQueryOperator(
        task_id='crear_table_fact_ventas',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS capa_bronze.fact_ventas_bancarias (
                venta_id SERIAL PRIMARY KEY,
                fecha_id INT,
                cliente_id INT,
                producto_id INT,
                ejecutivo_id INT,
                monto_apertura NUMERIC(15, 2),
                tasa_interes NUMERIC(5, 2)
            );
        """,
    )

    # Definir el orden de ejecución en paralelo para las dimensiones
    crear_esquema >> [crear_dim_clientes, crear_dim_productos, crear_dim_ejecutivos] >> crear_fact_ventas