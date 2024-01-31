FROM python:3.8-slim

# Copia los archivos necesarios al contenedor
COPY requirements.txt /app/requirements.txt
COPY . .
# Establece el directorio de trabajo
WORKDIR /app

# Instala las dependencias
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install currencyapicom

USER airflow

# Comando para ejecutar Airflow
CMD ["airflow", "scheduler", "-D"]
