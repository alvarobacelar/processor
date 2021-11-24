from kafka import KafkaProducer
from modules.enums import kafkaSetup
from modules.utils import logging
from json import dumps

producer = KafkaProducer(
  bootstrap_servers=[kafkaSetup.BROKER],
  client_id=kafkaSetup.CLIENT_ID,
  value_serializer=lambda x: dumps(x).encode("utf-8")
)

# pub result in kafka topic
def publish_kafka(data):
  pub_event = producer.send(kafkaSetup.TOPIC_TARGET, value=data)
  producer.flush()
  logging.info("Publish event in topic %s, partition %s and offset %s", pub_event.value.topic, pub_event.value.partition, pub_event.value.offset)