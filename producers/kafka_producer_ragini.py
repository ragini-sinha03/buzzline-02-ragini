"""
kafka_producer_ragini.py

Produce some custom streaming buzz strings (Ragini version) and send them to a Kafka topic.
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

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger


#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "ragini_topic")
    logger.info(f"[Ragini Producer] Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 2))
    logger.info(f"[Ragini Producer] Message interval: {interval} seconds")
    return interval


#####################################
# Message Generator
#####################################


def generate_messages(producer, topic, interval_secs):
    """
    Generate a stream of Ragini buzz messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.

    """
    ragini_messages: list = [
        "Hello from Ragini’s Kafka producer!",
        "Data streaming made easy with Kafka.",
        "Learning Kafka step by step.",
        "This is a custom buzz message.",
        "Ragini says: Python + Kafka = ❤️",
        "Streaming for data analytics practice.",
        "Another message from Ragini producer.",
        "Kafka pipelines are powerful!",
        "Final Ragini test message 🚀",
    ]
    try:
        while True:
            for message in ragini_messages:
                logger.info(f"[Ragini Producer] Generated buzz: {message}")
                producer.send(topic, value=message)
                logger.info(f"[Ragini Producer] Sent to topic '{topic}': {message}")
                time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("[Ragini Producer] Interrupted by user.")
    except Exception as e:
        logger.error(f"[Ragini Producer] Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("[Ragini Producer] Kafka producer closed.")


#####################################
# Main Function
#####################################
def main():
    print("Producer script started!")  # Add this line
    ...


def main():
    """
    Main entry point for Ragini’s producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams Ragini’s buzz message strings to the Kafka topic.
    """
    logger.info("[Ragini Producer] START.")
    logger.info("[Ragini Producer] Loading environment variables...")
    load_dotenv()
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("[Ragini Producer] Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"[Ragini Producer] Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"[Ragini Producer] Failed to create/verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"[Ragini Producer] Starting production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("[Ragini Producer] END.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
