import apache_beam as beam

# total schemes
TOTAL = 0

# Schemes_generating_returns_more_than_fd
good_schemes = 0

class filter(beam.DoFn):
    pass
    

def save_total(ele):
    global TOTAL
    TOTAL = ele

def save_good_schemes_total(ele):
    global good_schemes
    good_schemes = ele
    

def filter_good_ones(ele):
    cagr = list(ele.split(','))[-1]
    if float(cagr) > 7:
        return True
    return False

with beam.Pipeline() as pipeline:
    output = (  pipeline
            |   beam.io.ReadFromText('./scheme-details-cagr.csv')
            |   beam.combiners.Count.Globally()
            |   beam.Map(save_total)
    )


with beam.Pipeline() as pipeline:
    output = (  pipeline
            |   beam.io.ReadFromText('./scheme-details-cagr.csv')
            |   beam.Filter(filter_good_ones)
            |   beam.combiners.Count.Globally()
            |   beam.Map(save_good_schemes_total)
    )


op = str((int(good_schemes) / int(TOTAL))*100)
op = "{:.2f}".format(float(op))
print(op + ' percent schemes generates more returns than Fixed Deposits')

