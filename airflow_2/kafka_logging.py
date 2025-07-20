import logging
import json
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
            consumer_timeout_ms=5000  # 5s timeout
        )
        logger.info(f"✅ Successfully connected to Kafka topic '{topic}'")
        consumer.close()
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        raise e

def main():
    logger.info("========== Starting Flink Kafka JSON Streaming ==========")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    logger.info("Setting Kafka consumer properties...")
    properties = {
        'bootstrap.servers': '35.233.66.87:9092'
    }
    
    test_kafka_connection('35.233.66.87:9092', 'debezium.public.quotes_accesibility')

    logger.info("Creating Kafka consumer...")
    kafka_consumer = FlinkKafkaConsumer(
        topics='debezium.public.quotes_accesibility',
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )

    logger.info("Adding Kafka source to the stream...")
    kafka_stream = env.add_source(kafka_consumer)

    logger.info("Applying parsing and filtering...")
    parsed_stream = (
        kafka_stream
        .map(parse_json, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
    )

    logger.info("Mapping parsed stream to extract payload...")
    parsed_stream.map(lambda x: print_payload(x))

    logger.info("Submitting Flink job...")
    env.execute("Flink Kafka JSON Streaming")

    logger.info("========== Flink Job Completed ==========")

if __name__ == '__main__':
    main()
