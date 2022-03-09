from datetime import datetime
import apache_beam as beam
from apache_beam import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions



options = PipelineOptions()
options.view_as(StandardOptions).streaming = True
pipe = beam.Pipeline(options=options)

right_now = int(datetime.now().timestamp())

    
op = (  pipe
    |   TestStream()
        .add_elements([("default" , 0)] , event_timestamp = right_now)
        # .advance_processing_time(100)
        .add_elements([("default" , 150)] , event_timestamp = right_now + 150)
        # .advance_processing_time(100)
        .add_elements([("default" , 250)] , event_timestamp = right_now + 250)
        
    |   beam.WindowInto(window.FixedWindows(100))
    |   beam.GroupBy(lambda x : x[0])
    |   beam.Map(print) 
)

pipe.run()


