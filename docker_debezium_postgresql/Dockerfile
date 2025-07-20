FROM python:3.11.7

WORKDIR /app

COPY crear_tabla.py .

RUN pip install --upgrade pip
RUN pip install confluent_kafka
RUN pip install psycopg2