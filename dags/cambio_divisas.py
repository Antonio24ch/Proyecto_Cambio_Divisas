import requests
import json
import pytz
import os
import logging
import pandas as pd
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine, text
import psycopg2
from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import inspect

default_args = {
    'owner': 'antonio.chavez',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


WAREHOUSE_PORT=5439
standard_conforming_strings='on'
AIRFLOW_UID=50000
WAREHOUSE_HOST     = os.environ.get('WAREHOUSE_HOST')
WAREHOUSE_DBNAME   = os.environ.get('WAREHOUSE_DBNAME')
WAREHOUSE_USER     = os.environ.get('WAREHOUSE_USER')
WAREHOUSE_PASSWORD = os.environ.get('WAREHOUSE_PASSWORD')
api_key = os.environ.get('api_key')

cadena_conexion = f"postgresql://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DBNAME}"


def get_data(api_key):
    timezone = pytz.timezone('America/Mexico_City')
    current_time = datetime.now(timezone)
    formatted_date = current_time.strftime("%Y-%m-%d")
    currencies_list = ['EUR', 'GBP', 'USD', 'PEN', 'BTC', 'KRW', 'INR', 'CNY', 'BRL', 'ARS', 'JPY']
    currencies = ','.join(currencies_list)

    base_url = f"https://api.apilayer.com/currency_data/timeframe?start_date={formatted_date}&end_date={formatted_date}&source=MXN&currencies={currencies}"

    payload = {}
    headers = {
        "apikey": api_key
    }

    try:
        response = requests.get(base_url, headers=headers, data=payload)
        response.raise_for_status()  
        datos = response.json()

        # Convertir el DataFrame a una lista de diccionarios
        result = datos.get('quotes', [])
        return result
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud a la API: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error al decodificar la respuesta JSON: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado: {e}")
        return None


def transform_data(api_key):
    # Obtener el DataFrame utilizando la función get_data
    data_df = get_data(api_key)

    if data_df is not None and 'quotes' in data_df:
        df_quotes = pd.json_normalize(data_df['quotes'])
        df = pd.concat([data_df, df_quotes], axis=1).drop('quotes', axis=1)

        combined_row = df.iloc[0].combine_first(df.iloc[1])
        df_combined = pd.DataFrame([combined_row])

        df_combined.rename(columns=lambda x: x.replace('MXN', '') if 'MXN' in x else x, inplace=True)
        df_combined.rename(columns=lambda x: x.replace('start_date', 'date') if 'start_date' in x else x, inplace=True)

        columns_to_drop = ['success', 'timeframe', 'end_date']
        df_clean = df_combined.drop(columns=columns_to_drop).reset_index(drop=True)

        return df_clean
    else:
        return None

# Llamar a la función transform_data con tu clave API
transform_data(api_key)


def load_data(df, cadena_conexion):
    connection = None
    try:
        # cadena_conexion = f"postgresql://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DBNAME}?sslmode=require"
        connection = psycopg2.connect(cadena_conexion)
        cursor = connection.cursor()

        # Utilizar SQLAlchemy para cargar datos, sin crear la tabla
        engine = create_engine(cadena_conexion)
        df.to_sql('cambios_divisa', engine, if_exists='append', index=False)

        print("Datos cargados exitosamente en la tabla cambios_divisa.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

# Supongamos que transform_data toma api_key como argumento
df_clean = transform_data(api_key)

# Luego, llama a la función load_data con df_clean y las credenciales como argumentos
load_data(df_clean, cadena_conexion)

    
def update_database(cadena_conexion):
    # Comprobar que los datos fueron cargados exitosamente en redshift al ser llamados
    engine = create_engine(cadena_conexion)
    query = "SELECT * FROM cambios_divisa"
    existing_data = pd.read_sql(query, engine)

    existing_data.loc[existing_data['modified_date'].isnull(), 'modified_date'] = datetime.now()

    # Convertir la columna 'date' a tipo datetime
    existing_data['date'] = pd.to_datetime(existing_data['date'])

    # Formatear la columna 'date' para mostrar solo la fecha
    existing_data['date'] = existing_data['date'].dt.strftime('%Y-%m-%d')

    # Ordenar el DataFrame por 'date' y 'modified_date' de forma descendente
    existing_data = existing_data.sort_values(by=['date', 'modified_date'], ascending=[False, False])

    # Obtener el índice del primer valor máximo de la columna 'modified_date' para cada 'date'
    idx_max_modified_date = existing_data.groupby('date')['modified_date'].transform(max) == existing_data['modified_date']

    # Seleccionar las filas correspondientes a los índices obtenidos
    df_combined = existing_data[idx_max_modified_date]

    # Convertir 'date' a tipo datetime
    df_combined['date'] = pd.to_datetime(df_combined['date'])

    # Cargar los datos en la tabla 'cambios_divisa'
    df_combined['modified_date'] = datetime.now()
    df_combined.to_sql('cambios_divisa', engine, if_exists='replace', index=False)

    return df_combined


# Llamar a la función con la cadena de conexión
cadena_conexion = f"postgresql://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DBNAME}?sslmode=require"
result_df = update_database(cadena_conexion)




# Define el DAG
dag = DAG(
    'currency_data_dag',
    default_args=default_args,
    description='DAG for fetching currency data and updating the database',
    schedule_interval='0 0 * * *',
    max_active_runs=1,  # Establece el número máximo de ejecuciones activas a 1
)

# Define las tareas
get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    op_args=[api_key],  # Pasa la clave API como argumento
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[api_key],  # Pasa la clave API como argumento
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=[df_clean, cadena_conexion],  # Pasa el DataFrame y cadena de conexión como argumentos
    provide_context=True,
    dag=dag,
)

update_database_task = PythonOperator(
    task_id='update_database',
    python_callable=update_database,
    op_args=[cadena_conexion],  # Pasa la cadena de conexión como argumento
    provide_context=True,
    dag=dag,
)


# Define las dependencias entre tareas
get_data_task >> transform_data_task >> load_data_task >> update_database_task