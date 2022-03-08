from ast import For
from datetime import datetime, timedelta
from sqlite3 import DatabaseError
from threading import local
import apache_beam as beam
from datetime import datetime , timedelta
from apache_beam import window


def format_data(element):
    data = element.split(',')
    time = datetime.strptime(data[0], '%d-%b-%y').timestamp()
    return (time , data[0] , data[1] , data[2] , data[3] , data[4])

def show_data(element , type):
    print(type + " " + str(len(element[1])))
    return element

def calculate_mean(element):
    data = element[1]

    global mean 
    def save_ans(ans):
        global mean
        mean = ans

    with beam.Pipeline() as pipe:
        op = (  pipe
            |   beam.Create(data)
            |   beam.Map(lambda x : int(float(x[5])))
            |   beam.combiners.Mean.Globally()
            |   beam.Map(save_ans)
        )

    return mean


Main = beam.Pipeline()


Formated_data = (  Main
    |   beam.io.ReadFromText('D:/Desktop/Raj Desktop/apache beam practise/Beam windowing/nifty-data.csv')
    |   beam.Map(format_data)
    |   beam.Map(lambda x : window.TimestampedValue(x, x[0]))
)

Fixed_window = (    Formated_data
    |   beam.WindowInto(window.FixedWindows(timedelta(days=365).total_seconds()))
    |   "group by all elements of fixed window"  >>  beam.GroupBy(lambda x: "default_key")
    |   "display"   >>  beam.Map(show_data , "Fixed Window")
    # |   beam.Map(print)
    # |   beam.io.WriteToText('temp' , file_name_suffix='.txt')
)

Sliding_window = (  Formated_data
    |   "sliding windows of 365 days begins every 1 day"  >>  beam.WindowInto(window.SlidingWindows(timedelta(days=365).total_seconds(), timedelta(days=1).total_seconds()))
    |   "group by all elements of sliding window"   >>  beam.GroupBy(lambda x: "default_key")
    # |   "print"   >>   beam.Map(show_data , "Sliding Window")
    |   "calculate moving average"  >>  beam.Map(calculate_mean) 
    # |   beam.Map(print)
    |   beam.io.WriteToText("./moving_avg" , file_name_suffix='.txt')
)



Main.run()