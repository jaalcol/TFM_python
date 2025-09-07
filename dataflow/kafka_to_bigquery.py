import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logging.basicConfig(level=logging.INFO)

def parse_message(element):
    """Convierte el mensaje de Kafka de bytes a dict JSON"""
    value = element[1].decode('utf-8')
    try:
        return [json.loads(value)]
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON: {value}, Error: {e}")
        return []

def run(argv=None):
    options = PipelineOptions(argv)

    # Configuración GCP
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'proyecto1-469810'
    google_cloud_options.job_name = 'flink-to-bq-pipeline'
    google_cloud_options.staging_location = 'gs://tfm_bucket_bigquery0/staging'
    google_cloud_options.temp_location = 'gs://tfm_bucket_bigquery0/temp'
    google_cloud_options.region = 'europe-west1'

    # Configuración de workers
    worker_options = options.view_as(WorkerOptions)
    worker_options.max_num_workers = 10
    worker_options.num_workers = 2

    # Opciones generales
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'
    standard_options.streaming = True

    # Tabla y esquema de BigQuery
    table_spec = "proyecto1-469810:javi.resultados_inferencia"
    schema = (
        "id:INTEGER, "
        "income:FLOAT, "
        "name_email_similarity:FLOAT, "
        "prev_address_months_count:FLOAT, "
        "current_address_months_count:FLOAT, "
        "customer_age:FLOAT, "
        "days_since_request:FLOAT, "
        "intended_balcon_amount:FLOAT, "
        "payment_type:STRING, "
        "zip_count_4w:FLOAT, "
        "velocity_6h:FLOAT, "
        "velocity_24h:FLOAT, "
        "velocity_4w:FLOAT, "
        "bank_branch_count_8w:FLOAT, "
        "date_of_birth_distinct_emails_4w:FLOAT, "
        "employment_status:STRING, "
        "credit_risk_score:FLOAT, "
        "email_is_free:FLOAT, "
        "housing_status:STRING, "
        "phone_home_valid:FLOAT, "
        "phone_mobile_valid:FLOAT, "
        "bank_months_count:FLOAT, "
        "has_other_cards:FLOAT, "
        "proposed_credit_limit:FLOAT, "
        "foreign_request:FLOAT, "
        "source:STRING, "
        "session_length_in_minutes:FLOAT, "
        "device_os:STRING, "
        "keep_alive_session:FLOAT, "
        "device_distinct_emails_8w:FLOAT, "
        "device_fraud_count:FLOAT, "
        "month:FLOAT, "
        "score:FLOAT, "
        "probability:FLOAT"
    )


    kafka_config = {
        'bootstrap.servers': '34.52.238.189:9092',
        'auto.offset.reset': 'earliest'
    }

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Kafka" >> ReadFromKafka(consumer_config=kafka_config, topics=['resultados_inferencia'])
            | "Parse message" >> beam.FlatMap(parse_message)
            | "Write to BigQuery" >> WriteToBigQuery(
                table=table_spec,
                schema=schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location='gs://tfm_bucket_bigquery0/temp'
            )
        )

if __name__ == "__main__":
    run()
