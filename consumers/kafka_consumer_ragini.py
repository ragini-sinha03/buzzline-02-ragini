"""
kafka_consumer_ragini.py

Consume custom streaming buzz strings (Ragini version) from a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Import functions from local modules
from utils.utils_logger import logger

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "ragini_topic")
    logger.info(f"[Ragini Consumer] Kafka topic: {topic}")
    return topic

#####################################
# Message Consumer
#####################################

def consume_messages(consumer, topic):
    """
    Consume a stream of messages from a Kafka topic.

    Args:
        consumer (KafkaConsumer): The Kafka consumer instance.
        topic (str): The Kafka topic to consume messages from.
    """
    try:
        logger.info(f"[Ragini Consumer] Listening to topic '{topic}'...")
        for message in consumer:
            logger.info(f"[Ragini Consumer] Received: {message.value.decode('utf-8')}")
    except KeyboardInterrupt:
        logger.warning("[Ragini Consumer] Interrupted by user.")
    except Exception as e:
        logger.error(f"[Ragini Consumer] Error in message consumption: {e}")
    finally:
        consumer.close()
        logger.info("[Ragini Consumer] Kafka consumer closed.")

#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for Ragini’s consumer.

    - Loads environment variables.
    - Creates a Kafka consumer.
    - Consumes messages from the Kafka topic.
    """
    logger.info("[Ragini Consumer] START.")
    logger.info("[Ragini Consumer] Loading environment variables...")
    load_dotenv()

    topic = get_kafka_topic()

    # Create the Kafka consumer
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            auto_offset_reset='earliest',
            group_id='ragini-consumer-group',
            enable_auto_commit=True
        )
    except Exception as e:
        logger.error(f"[Ragini Consumer] Failed to create Kafka consumer: {e}")
        sys.exit(3)

    # Consume messages
    consume_messages(consumer, topic)

    logger.info("[Ragini Consumer] END.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
