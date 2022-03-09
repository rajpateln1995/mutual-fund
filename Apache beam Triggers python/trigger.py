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








# from datetime import datetime
# from unittest import TestCase, main

# from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
# import apache_beam as beam
# from apache_beam.testing.test_pipeline import TestPipeline
# from apache_beam.testing.test_stream import TestStream
# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.transforms.trigger import AccumulationMode, Repeatedly, AfterProcessingTime


# class FormatResult(beam.DoFn):
#     def process(self, team_score, window=beam.DoFn.WindowParam):
#         team_name, score = team_score
#         yield {
#             'team': team_name,
#             'score': score,
#             'eventTime': (window.start, window.end)
#         }


# class CalculateTeamScores(beam.PTransform):
#     def expand(self, scores):
#         return (scores
#                 | "windowing scores" >> beam.WindowInto(
#                     beam.window.FixedWindows(120),
#                     trigger=Repeatedly(AfterProcessingTime(120)),
#                     accumulation_mode=AccumulationMode.ACCUMULATING)
#                 | "preparing scores for combining" >> beam.Map(
#                     lambda team_score: (team_score['team'], team_score['score']))
#                 | "calculating team scores" >> beam.CombinePerKey(sum)
#                 | "forming the result" >> beam.ParDo(FormatResult()))


# def timestamp_from_datetime(t: str):
#     dt = '1970-01-01 ' + t
#     dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
#     return int(dt.timestamp())


# def team_score(team_name, score):
#     return {
#         'team': team_name,
#         'score': score
#     }


# class TestCalculateTeamScores(TestCase):

#     def test_should_sum_score_for_each_team(self):
#         # given
#         options = PipelineOptions()
#         options.view_as(StandardOptions).streaming = True
#         p = TestPipeline(options=options)

#         scores_stream = (p | "loading score stream" >> TestStream()
#                          .add_elements(elements=[team_score('red', 5)],
#                                        event_timestamp=timestamp_from_datetime('12:00:30'))
#                          .add_elements(elements=[team_score('red', 7)],
#                                        event_timestamp=timestamp_from_datetime('12:02:20'))
#                          .add_elements(elements=[team_score('red', 3)],
#                                        event_timestamp=timestamp_from_datetime('12:03:50'))
#                          .add_elements(elements=[team_score('red', 4)],
#                                        event_timestamp=timestamp_from_datetime('12:04:20'))
#                          .advance_processing_time(120)
#                          .add_elements(elements=[team_score('red', 8)],
#                                        event_timestamp=timestamp_from_datetime('12:03:10'))
#                          .add_elements(elements=[team_score('red', 3)],
#                                        event_timestamp=timestamp_from_datetime('12:06:50'))
#                          .add_elements(elements=[team_score('red', 9)],
#                                        event_timestamp=timestamp_from_datetime('12:01:50'))
#                          .add_elements(elements=[team_score('red', 8)],
#                                        event_timestamp=timestamp_from_datetime('12:07:30'))
#                          .add_elements(elements=[team_score('red', 1)],
#                                        event_timestamp=timestamp_from_datetime('12:07:50'))
#                          .advance_processing_time(120))

#         # when
#         result = scores_stream | 'calculating team scores' >> CalculateTeamScores()

#         # then
#         assert_that(result, equal_to([
#             {
#                 'team': 'red',
#                 'score': 5,
#                 'eventTime': (timestamp_from_datetime('12:00:00'),
#                               timestamp_from_datetime('12:02:00'))
#             },
#             {
#                 'team': 'red',
#                 'score': 14,
#                 'eventTime': (timestamp_from_datetime('12:00:00'),
#                               timestamp_from_datetime('12:02:00'))
#             },

#             {
#                 'team': 'red',
#                 'score': 10,
#                 'eventTime': (timestamp_from_datetime('12:02:00'),
#                               timestamp_from_datetime('12:04:00'))
#             },
#             {
#                 'team': 'red',
#                 'score': 18,
#                 'eventTime': (timestamp_from_datetime('12:02:00'),
#                               timestamp_from_datetime('12:04:00'))
#             },

#             {
#                 'team': 'red',
#                 'score': 4,
#                 'eventTime': (timestamp_from_datetime('12:04:00'),
#                               timestamp_from_datetime('12:06:00'))
#             },

#             {
#                 'team': 'red',
#                 'score': 12,
#                 'eventTime': (timestamp_from_datetime('12:06:00'),
#                               timestamp_from_datetime('12:08:00'))
#             },
#         ]))
#         p.run()


# if __name__ == '__main__':
#     main()