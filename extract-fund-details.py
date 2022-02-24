import apache_beam as beam
import json


class extract_data(beam.DoFn):

    def process(self, element):


        try:
            scheme_name , scheme_code = element.split(',')
            li = [scheme_name , scheme_code]
            with open('mutual-funds-data/' + scheme_name + '.txt', "r") as file:
                data = file.read()
                data = data.replace("'" , '"')
                data = json.loads(data)
                
                li.append(data['meta']['fund_house'])
                li.append(data['meta']['scheme_type'])
                li.append(data['meta']['scheme_category'])
                li.append(data['meta']['scheme_code'])
                li.append(data['meta']['scheme_name'])

            yield ','.join(map(str, li))
        except Exception as Argument:
            with open("runtime_logs_fund_details.txt" ,'a') as file:
                temp = str(Argument)
                for i in li:
                    temp += str(i)
                file.write(temp + '\n')



with beam.Pipeline() as pipeline:
    output = ( pipeline 
        |   beam.io.ReadFromText('Mutual-fund-list-formatted.csv')
        |   beam.ParDo(extract_data())
        |   beam.io.WriteToText('./Scheme-details-cagr' , file_name_suffix='.csv')
    )
