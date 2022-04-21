from prometheus_client import Summary
from fluvio import (Fluvio, Offset)
fluvio = Fluvio.connect()
partition = 0
consumer = fluvio.partition_consumer("hello-python", partition)
#TODO: below approach is only valid if the whole stream in memory already
for i in consumer.stream(Offset.beginning()):
    print("Received message: %s" % i.value_string())

