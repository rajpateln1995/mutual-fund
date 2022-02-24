import apache_beam as beam
import json



def loadjson(ele):
    temp = json.loads(ele)                          
    for t in temp:
        scheme_name = ""
        illegal_char = ["/", "\\" , '!' , '@' , '#' , '$' , '%' , '^', '&' , '*' , '(' , ')' , '+' , '=' , '[' , ']' , '{' , '}' , '|' , ':' , ';' , '<' , '>' , '?' , '"' , "'" , ',']
        for i in t['schemeName']:
            if i in illegal_char:
                scheme_name += " "
            else:
                scheme_name += i

        yield str(scheme_name) + "," + str(t['schemeCode']).strip()
    


with beam.Pipeline() as pipeline:
    output = (
        pipeline 
        |   "loading json file" >> beam.io.ReadFromText('Mutual-fund-list-unformatted.txt')
        |   "format to csv" >> beam.FlatMap(loadjson)
        |   "storing output" >> beam.io.WriteToText('Mutual-fund-list-formatted' , file_name_suffix='.csv')
    )