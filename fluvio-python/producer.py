import datetime
from fluvio import Fluvio
fluvio = Fluvio.connect()

producer = fluvio.topic_producer("hello-python")
partition = 0
while True:
    line = input('> ')
    event_time = datetime.datetime.now()
    print("Event-time: %s" % event_time)
    producer.send_string(line)
