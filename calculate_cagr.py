from tracemalloc import start
import apache_beam as beam
import json
from datetime import date


class getCAGR(beam.DoFn):
    
    def process(self, element):
        try:
            li = list(element.split(','))
            with open('mutual-funds-data/' + li[0] + '.txt', "r") as file:
                data = file.read()
                data = data.replace("'" , '"')
                data = json.loads(data)
                
                end_date = data["data"][0]["date"].split('-')
                end_date_nav = data["data"][0]["nav"]
                start_date = data["data"][-1]["date"].split('-')
                start_date_nav = data["data"][-1]["nav"]
                start_date = date(int(start_date[2]) , int(start_date[1]) , int(start_date[0]))
                end_date = date(int(end_date[2]) , int(end_date[1]) , int(end_date[0]))
                time_difference = end_date - start_date
                difference_in_days = time_difference.days
                difference_in_years = difference_in_days/365

                cagr = (float(end_date_nav)/float(start_date_nav))**(1/float(difference_in_years))-1

                li.append(difference_in_years)
                li.append(cagr)
                li.append(start_date)
                li.append(end_date)
                

            yield ','.join(map(str, li))
        except Exception as Argument:
            with open("runtime_logs_calculate_cagr.txt" ,'a') as file:
                temp = str(Argument)
                for i in li:
                    temp += str(i)
                file.write(temp + '\n')


with beam.Pipeline() as pipeline:
    output = ( pipeline 
        |   beam.io.ReadFromText('Scheme-details.csv')
        |   beam.ParDo(getCAGR())
        |   beam.io.WriteToText('./Scheme-details' , file_name_suffix='.csv')
    )