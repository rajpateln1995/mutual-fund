import apache_beam as beam
import requests


class fetch_funds_data(beam.DoFn):
    
    def process(self, element):
        fund_name , fund_code = element.split(',')
        r = requests.get('https://api.mfapi.in/mf/' + fund_code)
        

        try:
            filename = fund_name + '.txt'
            filename = "mutual-funds-data/" + filename
            with open(filename ,'w') as file:
                file.write(str(r.json()))
                print(fund_code)
                return None

        except Exception as Argument:
            with open("runtime_logs.txt" ,'a') as file:
                file.write(str(Argument) + '\n')
                print(fund_code)

        return None


      

with beam.Pipeline() as pipeline:
    output = (
        pipeline 
        |   beam.io.ReadFromText('Mutual-fund-list-formatted.csv')
        |   beam.ParDo(fetch_funds_data())
    )