from kafka import KafkaConsumer
from modules.enums import kafkaSetup
from modules.utils import logging
from modules.publish_kafka import publish_kafka
from modules.eventProcess import EventProcess
from json import loads


# Subscribe in kafka topic
def RunProcessor():
  consumer = KafkaConsumer(
    bootstrap_servers=[kafkaSetup.BROKER],
    enable_auto_commit=kafkaSetup.OFFSET_COMMIT,
    group_id=kafkaSetup.GROUP_ID,
    client_id=kafkaSetup.CLIENT_ID,
    auto_offset_reset=kafkaSetup.OFFSET_RESET,
    value_deserializer=lambda m: loads(m.decode('utf-8'))
  )

  consumer.subscribe([kafkaSetup.TOPIC_TAGS])

  logging.info("Subscribe on kafka cluster %s", kafkaSetup.BROKER)
  for msg in consumer:
    logging.info("Consuming event on topic %s, partition %s and offset %s", kafkaSetup.TOPIC_TAGS, msg.partition, msg.offset)
    tags = msg.value
    # if value is isn't json, try to converter
    try:
      roller1 = tags["read"]["RD1_PV_VRM01_POSITION_ROLLER_1"]
      roller2 = tags["read"]["RD1_PV_VRM01_POSITION_ROLLER_2"]
      on_off  = tags["read"]["RD1_MD_VRM01_ON_OFF"]
      tags_json = tags
    except:
      tags_json = loads(tags)
      roller1 = tags_json["read"]["RD1_PV_VRM01_POSITION_ROLLER_1"]
      roller2 = tags_json["read"]["RD1_PV_VRM01_POSITION_ROLLER_2"]
      on_off  = tags_json["read"]["RD1_MD_VRM01_ON_OFF"]

    del tags_json["read"]["RD1_PV_VRM01_POSITION_ROLLER_1"]
    del tags_json["read"]["RD1_PV_VRM01_POSITION_ROLLER_2"]

    event_process = EventProcess(tags_json)
    
    # if RD1_MD_VRM01_ON_OFF not a number, it will be set zero and all values in json event will be set null 
    if type(on_off) != int:
      on_off = 0
      
    # Call function for do roller avg
    avg = event_process.processor_avg(roller1, roller2, on_off)

    # Call function for add avg in json event
    event_process = event_process.add_avg_json(tags_json, avg)

    # Call function for publish event processed in kafka
    publish_kafka(event_process)
