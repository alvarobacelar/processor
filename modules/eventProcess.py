from modules.utils import logging

"""
Class for do process each kafka event
"""
class EventProcess:

  def __init__(self, event):
        self.event = event

  def processor_avg(self, roller1, roller2, on_off):
    logging.info("Doing roller avg")
    if (on_off >= 1 ):
      avg_roller = roller1 + roller2 / 2
    else:
      avg_roller = None
      self.change_value_null(self.event)
    return avg_roller

  def change_value_null(self, value):
    for v in value["read"]:
      value["read"][v] = None
    return value

  def add_avg_json(self, value, avg):
    value["read"]["RD1_MD_AI_BAD_HEIGHT"] = avg
    return value