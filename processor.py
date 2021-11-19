from threading import Thread
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
import json
import threading
import logging
import time

logging.basicConfig(
        format='[%(asctime)s.%(msecs)s:%(name)s:%(thread)d]' +
               ' %(levelname)s - %(message)s',
        level=logging.INFO
    )

kafka_servers = "127.0.0.1:9092"
kafka_topic_src = "tags_values"
kafka_topic_dst = "tags_results"

producer = KafkaProducer(
    bootstrap_servers=[kafka_servers], 
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
  )

# Subscribe in kafka topic
def consumerKafka():
  consumer = KafkaConsumer(
    kafka_topic_src,
    bootstrap_servers=[kafka_servers],
    enable_auto_commit=True,
    group_id="processo-teste",
    client_id="processo-id",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
  )

  logging.info("Consuming from kafka")
  # Read events from kafka topics
  for msg in consumer:
    value = msg.value
    roller1 = value["read"]["RD1_PV_VRM01_POSITION_ROLLER_1"]
    roller2 = value["read"]["RD1_PV_VRM01_POSITION_ROLLER_2"]
    avg = do_avg(roller1, roller2)
    pub_result_kafka({"RD1_MD_AI_BAD_HEIGHT": avg})
    # TODO: se a variavel RD1_MD_VRM01_ON_OFF form menos igual a 0, colocar null nos valores

# do avg with kafka events
def do_avg(roller1, roller2):
  logging.info("Doing avg")
  avg_roller = roller1 + roller2 / 2
  return avg_roller

# pub result in kafka topic
def pub_result_kafka(data):
  logging.info("Publish result on kafka topic %s", kafka_topic_dst)
  producer.send(kafka_topic_dst, value=data)
  producer.flush()
    
def main():
    consumerKafka()

if __name__ == "__main__":
  main()