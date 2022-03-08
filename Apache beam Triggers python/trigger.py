import apache_beam as beam
from apache_beam.io.external.kafka import ReadFromKafka

with beam.Pipeline() as pipeline:
    
    op = (  pipeline
        |   ReadFromKafka(
            consumer_config={'bootstrap-servers' : 'localhost:9092'}, topics=['test-topic'])
        |   Map(print)
    )