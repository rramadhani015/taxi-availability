from airflow.models import Variable

from helpers.helper import query_file
import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

import json  
import base64 
from airflow.hooks.postgres_hook import PostgresHook
from pandas import json_normalize 
from helpers.helper import pg_to_df, query_file

db1 = PostgresHook('db1')

class TaxiReadApi():
    def __init__(self):
        self.api_conn = 'https://api.data.gov.sg/v1/transport/taxi-availability'

    def read_api(self):
        response = requests.get(self.api_conn)
        if response.status_code == 200:
            print("Succesful connection with API.")
            print('-------------------------------')
            self.data = response.json()
        elif response.status_code == 404:
            print("Unable to reach URL.")
        else:
            print("Unable to connect API or retrieve data.")

    def extract(self):
        self.df = json_normalize(self.data)
                  
    def transform(self):
        df = self.df        
        # key = old name
        # value = new name
        dict1 = {'type': 'type',
                'features': 'features',
                'crs.type': 'crs_type',
                'crs.properties.href': 'crs_properties_href',
                'crs.properties.type': 'crs_properties_type'}
        
        # call rename () method
        df.rename(columns=dict1,inplace=True)
        
        df_feature = json_normalize(self.data,'features')
        # type	geometry.type	geometry.coordinates	properties.timestamp	properties.taxi_count	properties.api_info.status
        dict2 = {'type': 'feature_type',
                'geometry.type': 'geometry_type',
                'geometry.coordinates': 'geometry_coordinates',
                'properties.timestamp': 'properties_timestamp',
                'properties.taxi_count': 'properties_taxi_count',
                'properties.api_info.status': 'properties_api_info_status'}
        
        # call rename () method
        df_feature.rename(columns=dict2,inplace=True)
        df_merged = df.assign(key=1).merge(df_feature.assign(key=1), on='key').drop('key',axis=1)

        df_merged['geometry_coordinates'] = df_merged['geometry_coordinates'].astype(str)
        # df_merged['features'] = '-'
        # df['features'] = df['features'].fillna('-').astype(str)
        # df['features'] = df['features'].fillna('-').astype(str)
        # df['features'] = df['features'].replace({'\'': "''"}, regex=True)
        # df['crs_type'] = df['crs_type'].fillna('-').astype(str)
        # df_merged['crs_properties_href'] = df_merged['crs_properties_href'].fillna('-').astype(str)
        # df_merged['crs_properties_type'] = df_merged['crs_properties_type'].fillna('-').astype(str)
        df_merged.drop(['features'], inplace=True, axis=1)
        # print(df_merged)
        self.df = df_merged

    def insert_to_db(self):  
        df1 = self.df           
        TARGET='db1.taxi_availability'
        sql_texts = []
        for index, row in df1.iterrows():       
            sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join(df1.columns))+ ') VALUES '+ str(tuple(row.values)))        
        # return sql_texts
        
        query1 =''
        for sql in sql_texts:
            query1 += sql+';'
        
        delete_query = f"""delete from {TARGET} where properties_timestamp = '{df1['properties_timestamp'][0]}';"""    
        query1= query1.replace('"',"'")
        query1= delete_query+query1
        return query1

def run_taxi_api_to_db():
    etl = TaxiReadApi()
    etl.read_api()
    etl.extract()
    etl.transform()
    query=etl.insert_to_db()
    db1.run(query)
    # print(query)
