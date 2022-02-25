import sqlite3

import apache_beam as beam


count = 0



def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        c = conn.cursor()
        return conn

        
    except Error as e:
        print(e)
        return None
    
    


connection = create_connection('db-file.db')

create_table_query = """
create table mutual_fund_schemes(Scheme_Name text,
    Scheme_Code text,
    Fund_House text,
    Scheme_Category text,
    Scheme_Type text,
    code text,
    name text,
    years text,
    cagr text,
    start_date text,
    end_date text,
    cagr_in_percentage text)"""




def store_to_db(element):
    sql = ''' INSERT INTO mutual_fund_schemes(Scheme_Name,Scheme_Code,Fund_House,Scheme_Category,Scheme_Type,code,name,years,cagr,start_date,end_date,cagr_in_percentage)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '''
    c = connection.cursor()
    c.execute(sql, element.split(','))
    
    connection.commit()
    global count
    print(count)
    count+=1
    return None




c = connection.cursor()
c.execute(create_table_query)
connection.commit()

with beam.Pipeline() as pipeline:

    output = (  pipeline
        |   beam.io.ReadFromText('scheme-details-cagr.csv')
        |   beam.Map(store_to_db)
    )


    
