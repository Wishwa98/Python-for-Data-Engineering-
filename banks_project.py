import datetime
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
import sqlite3


url= 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
Rate_csv='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')
    
    data_list = []  # Store rows as dictionaries in a list
    
    for row in rows:
        col = row.find_all('td')
        if col:
            bank_name = col[1].find_all("a")[1].text
            market_cap_str = col[2].text.replace(',', '').replace('\n', '')
            market_cap = float(market_cap_str)
            
            # Append row as a dictionary to the list
            data_list.append({"Name": bank_name, "MC_USD_Billion": market_cap})

    # Concatenate the list of dictionaries into a DataFrame
    df = pd.concat([df, pd.DataFrame(data_list)], ignore_index=True)
    
    return df



def transform(df):
    
    dataframe = pd.read_csv(Rate_csv)
    dict = dataframe.set_index('Currency').to_dict()['Rate']
    
    
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * dict['GBP'], 2)

    # Add column for Market Capitalization in EUR
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * dict['EUR'], 2)

    # Add column for Market Capitalization in INR
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * dict['INR'], 2)
        
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
    
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



#FINAL CALL
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement1=f'SELECT * FROM Largest_banks'
run_query(query_statement1,sql_connection)

query_statement2=f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement2,sql_connection)

query_statement3=f'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement3,sql_connection)

log_progress('Process Complete.')


