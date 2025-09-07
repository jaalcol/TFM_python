import logging
import json
import numpy as np
import os
import sys
import argparse
from tensorflow.keras.models import load_model
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from kafka import KafkaConsumer

# ---------------------------
# Set up logger
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

model = None  # Global model

def load_model_from_path(path):
    global model
    try:
        model = load_model(path)
        logger.info(f"Model loaded from: {path}")
    except Exception as e:
        logger.error(f"Failed to load model from {path}: {e}")
        model = None

# ---------------------------
# Original functions (unchanged)
# ---------------------------
def parse_json(line):
    logger.debug(f"Raw message: {line}")
    try:
        parsed = json.loads(line)
        logger.info(f"Parsed JSON: {parsed}")
        return parsed
    except Exception as e:
        logger.error(f"JSON parsing failed: {e}")
        return None

def print_payload(record):
    payload = record.get('payload')
    if payload:
        logger.info(f"Payload: {payload}")
    else:
        logger.warning(f"No 'payload' field in record: {record}")

def test_kafka_connection(bootstrap_servers, topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            consumer_timeout_ms=5000
        )
        logger.info(f"Successfully connected to Kafka topic '{topic}'")
        consumer.close()
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise e

# ---------------------------
# Inference
# ---------------------------
def run_inference(record):
    if model is None:
        logger.warning("Model not loaded. Skipping inference.")
        return None

    try:
        payload = record.get("payload", {})
        after = payload.get("after", {})
        features = after.get("features")

        if features is None:
            logger.warning(f"'features' missing in payload: {after}")
            return None

        X = np.array(features).reshape(1, -1)
        reconstruction = model.predict(X)
        error = float(np.mean(np.square(X - reconstruction)))

        result = {
            "features": features,
            "reconstruction_error": error,
            "anomaly": error > 0.1  # adjust threshold as needed
        }

        logger.info(f"Inference result: {result}")
        return result
    except Exception as e:
        logger.error(f"Inference failed: {e}")
        return None

# ---------------------------
# Main function
# ---------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", type=str, required=True, help="Path to the .h5 model file")
    args = parser.parse_args()

    load_model_from_path(args.model_path)

    logger.info("Starting Flink Kafka JSON Streaming")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    properties = {
        'bootstrap.servers': '34.79.169.157:9092'
    }

    test_kafka_connection('34.79.169.157:9092', 'debezium.public.quotes_accesibility')

    kafka_consumer = FlinkKafkaConsumer(
        topics='debezium.public.quotes_accesibility',
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )

    kafka_stream = env.add_source(kafka_consumer)

    parsed_stream = (
        kafka_stream
        .map(parse_json, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
    )

    inference_stream = (
        parsed_stream
        .map(run_inference, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
    )

    inference_stream.map(lambda x: logger.info(f"Output: {x}"))

    env.execute("Flink Kafka JSON Streaming")
    logger.info("Flink Job Completed")

if __name__ == '__main__':
    main()
