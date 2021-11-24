import logging
import json

# Setup logging 
logging.basicConfig(
  format='[%(asctime)s.%(msecs)s:%(name)s:%(thread)d]' +
         ' %(levelname)s - %(message)s',
  level=logging.INFO
)

def json_serializer(data):
    return lambda x: json.dumps(x).encode("utf-8")

def json_desializer(data):
    return lambda m: json.loads(m.decode('utf-8'))