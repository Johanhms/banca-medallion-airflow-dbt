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
    'orquestacion_esquema_johan',
    default_args=default_args,
    description='Pipeline para interactuar con test_db y el esquema practicas_johan',
    schedule_interval=None, 
    catchup=False,
) as dag:

    # Tarea 1: Asegurarnos de que el esquema exista en test_db
    crear_esquema = SQLExecuteQueryOperator(
        task_id='crear_esquema_practicas',
        conn_id='postgres_default',
        sql="CREATE SCHEMA IF NOT EXISTS practicas_johan;",
    )

    # Tarea 2: Crear una tabla dentro de ese esquema específico
    crear_tabla = SQLExecuteQueryOperator(
        task_id='crear_tabla_viajes',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS practicas_johan.registro_dags (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                nombre_dag VARCHAR(100),
                estado VARCHAR(50)
            );
        """,
    )

    # Tarea 3: Insertar un registro de prueba
    insertar_registro = SQLExecuteQueryOperator(
        task_id='insertar_datos_prueba',
        conn_id='postgres_default',
        sql="""
            INSERT INTO practicas_johan.registro_dags (nombre_dag, estado)
            VALUES ('orquestacion_esquema_johan', 'Exitoso desde Airflow');
        """,
    )

    # Flujo secuencial de tareas
    crear_esquema >> crear_tabla >> insertar_registro