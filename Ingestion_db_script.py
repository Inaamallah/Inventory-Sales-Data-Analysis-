import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename = 'logs/ingestion_db.log',
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine("sqlite:///inventory.db")

def raw_data():
    '''This Function loads CSVs File as Data frame and Ingest into DB'''
    start = time.time()
    for file in os.listdir('data/data'):
        if '.csv' in file:
            df = pd.read_csv('data/data/' + file)
            logging.info(f"Ingesting {file} in DB")
            ingest_db(df,file[:-4],engine)
    end = time.time()
    total_time = (end-start)/60
    logging.info('---------------INGESTION COMPLETED----------------')
    logging.info(f'\nTotal Time Taken to Ingest Data: {total_time} minutes')

def ingest_db(df,table_name,engine):
    '''This Function will ingest DF into DB'''
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)

if __name__ == '__main__':
    raw_data()