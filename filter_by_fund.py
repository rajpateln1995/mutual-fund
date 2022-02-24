import apache_beam as beam

Funds = dict()
Funds_score = dict()
Funds_total = dict()


def group_schemes(ele):
    li = list(ele.split(','))
    return li[2]

def store_schemes(ele):
    global Funds
    Funds[ele[0]] = ele[1]

def filter_good_ones(ele):
    cagr = list(ele.split(','))[-1]
    if float(cagr) > 7:
        return True
    return False





with beam.Pipeline() as pipeline:
    output = (  pipeline
            |   beam.io.ReadFromText('./scheme-details-cagr.csv')
            |   beam.GroupBy(group_schemes)
            |   beam.Map(store_schemes)
    )

# save total for all
for fund_name , fund_details in Funds.items():
    Funds_total[fund_name] = len(fund_details)

for fund_name , fund_details in Funds.items():

    def save_score(ele):
        Funds_score[fund_name] = ele

    with beam.Pipeline() as pipeline:
        output = (  pipeline
                |   beam.Create(fund_details)
                |   beam.Filter(filter_good_ones)
                |   beam.combiners.Count.Globally()
                |   beam.Map(save_score)
        )

results = dict()

for fund_name , score in Funds_score.items():
    results[fund_name] = (int(score) / int(Funds_total[fund_name]))*100

for name, score in sorted(results.items() , key=lambda item: item[1]):
    print(str(name) , " " , str(score) + "%")
    