import os
import currencyapicom
import logging
import json
import pandas as pd
import subprocess
subprocess.run(['pip', 'install', 'psycopg2-binary'])
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


import pytz


doc_md = """
## Refresh of staging schemas in Redshift
## SUMARY:
-----
- DAG Name:
    `crypto_data`
- Owner:
    `Antonio`
### Description:
    Este proceso extrae datos sobre los cambios de divisas de diferentes monedas, 
    tomando como base el peso mexicano, los datos son extraidos a través de una API que se
    consume diarimente para posteriormente cargarlos a una tabla en redshift.
"""


# ---------- Globals ---------------
dag_id = "cambio_divisas_entregable3"
schedule_interval = "@daily"
default_args = {
    "owner": "antonio.chavez",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# -------- Variables ---------------------
dwh_host = Variable.get("DB_HOST")
dwh_user = Variable.get("DB_USER")
dwh_name = Variable.get("DB_NAME")
dwh_port = Variable.get("DB_PORT") 
dwh_password = Variable.get("DB_PASSWORD")
dwh_schema=Variable.get("DB_SCHEMA")
api_key = Variable.get("API_KEY")
standard_conforming_strings='on'
AIRFLOW_UID=50000
table_name = "cambios_divisa"
base_currency = 'MXN' 
currencies = ['EUR', 'GBP', 'USD', 'PEN', 'BTC', 'KRW', 'INR', 'CNY', 'BRL', 'ARS', 'JPY']



def transform_data(**context):
    try:
        client = currencyapicom.Client(api_key)
        result = client.latest(base_currency, currencies=['EUR', 'GBP', 'USD', 'PEN', 'BTC', 'KRW', 'INR', 'CNY', 'BRL', 'ARS', 'JPY'])
        result_json = json.dumps(result, indent=2)
        print(result_json)
        # Limpiar datos
        df = pd.json_normalize(json.loads(result_json))
        date = df['meta.last_updated_at'].values[0]
        result_df = pd.DataFrame({
            'date': [date],
            'base_currency': [base_currency],
            'EUR': [df['data.EUR.value'].values[0]],
            'GBP': [df['data.GBP.value'].values[0]],
            'USD': [df['data.USD.value'].values[0]],
            'PEN': [df['data.PEN.value'].values[0]],
            'BTC': [df['data.BTC.value'].values[0]],
            'KRW': [df['data.KRW.value'].values[0]],
            'INR': [df['data.INR.value'].values[0]],
            'CNY': [df['data.CNY.value'].values[0]],
            'BRL': [df['data.BRL.value'].values[0]],
            'ARS': [df['data.ARS.value'].values[0]],
            'JPY': [df['data.JPY.value'].values[0]]
        }, index=[0])

        # Convertir la columna 'date' a datetime con formato UTC
        result_df['date'] = pd.to_datetime(result_df['date'], utc=True)

        # Formatear la columna 'date' como cadena de texto sin información de zona horaria
        result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        result_df['date'] = pd.to_datetime(result_df['date'])
        print(result_df)
        csv_filename = f"{context['ds']}_stocks_data.csv"
        result_df.columns = result_df.columns.str.lower()
        result_df.to_csv(csv_filename, index=False)
        print(f"Archivo CSV guardado en: {os.path.abspath(csv_filename)}")
        return csv_filename 
    except Exception as e:
        logging.error(f"Error al construir Data Frame: {str(e)}")
        logging.exception("Detalles completos de la excepción:")
        return None



def save_data_to_dw(**context):
    try:
        # Obtener el nombre del archivo CSV del contexto
        csv_filename = context['ti'].xcom_pull(task_ids='get_and_transform_data')
        df = pd.read_csv(csv_filename)

        # Utilizar credenciales
        username = dwh_user
        password = dwh_password
        host = dwh_host
        port = dwh_port
        database = dwh_name

        # Construir la cadena de conexión
        cadena_conexion = f"postgresql://{username}:{password}@{host}:{port}/{database}"

        # Conectar al motor de la base de datos
        engine = create_engine(cadena_conexion)

        # Realizar la operación de escritura en la base de datos
        df.to_sql('cambios_divisa', engine, if_exists='append', index=False)

        # Mensaje de éxito
        print("¡Todos los datos fueron insertados correctamente!")

    except Exception as e:
        logging.error(f"Error al insertar datos en la base de datos: {str(e)}")
        logging.exception("Detalles completos de la excepción:")
        raise e


def update_data_to_dw(**context):
    try:
        # Utilizar credenciales 
        username = dwh_user
        password = dwh_password
        host = dwh_host
        port = dwh_port
        database = dwh_name

        # Construir la cadena de conexión
        cadena_conexion = f"postgresql://{username}:{password}@{host}:{port}/{database}"

        # Transformas datos
        engine = create_engine(cadena_conexion)
        query = "SELECT * FROM cambios_divisa"
        existing_data = pd.read_sql(query, engine)
        existing_data.loc[existing_data['modified_date'].isnull(), 'modified_date'] = datetime.now()

        # Convertir la columna 'date' a tipo datetime
        existing_data['date'] = pd.to_datetime(existing_data['date'])

        # Formatear la columna 'date' para mostrar solo la fecha
        existing_data['date'] = existing_data['date'].dt.strftime('%Y-%m-%d')

        # Ordenar el DataFrame por 'date' de forma descendente y 'modified_date' de forma descendente
        existing_data = existing_data.sort_values(by=['date', 'modified_date'], ascending=[False, False])

        # Eliminar duplicados basados en la columna 'date' y conservar el primero (el más reciente)
        df_combined = existing_data.loc[existing_data.groupby('date')['modified_date'].idxmax()]
        df_combined['date'] = pd.to_datetime(df_combined['date'])

        df_combined['modified_date'] = datetime.now()
        df_combined.to_sql('cambios_divisa', engine, if_exists='replace', index=False)

        # Mensaje de éxito
        print("¡Todos los datos fueron actualizados!")

    except Exception as e:
        logging.error(f"Error al insertar datos en la base de datos: {str(e)}")
        logging.exception("Detalles completos de la excepción:")
        raise e




with DAG(
    dag_id=dag_id,
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=schedule_interval,
    start_date=datetime(2024,1,27),
    description="Este proceso extrae información de las criptodivisas desde CoinAPI y las guarda en las tablas crypto & crypto_stg a las 00:00 HRS todos los días",
    catchup=True,
    doc_md=doc_md,
    tags=["staging"]
) as dag:
    # -------------- Tasks ----------------

    get_and_transform_data = PythonOperator(
        task_id="get_and_transform_data",
        python_callable=transform_data,
        dag=dag, 
        provide_context=True
    )


    load_data = PythonOperator(
        task_id="load_data",
        python_callable=save_data_to_dw,
        dag=dag, 
        provide_context=True,
        trigger_rule="all_success"
    )

    
    update_data = PythonOperator(
        task_id="update_data",
        python_callable=update_data_to_dw,
        dag=dag, 
        provide_context=True,
        trigger_rule="all_success"
    )

    get_and_transform_data >> load_data >> update_data