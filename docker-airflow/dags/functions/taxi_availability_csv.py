from datetime import datetime
import os
import requests
import pandas as pd
from pandas import json_normalize


class TaxiReadApi:
    def __init__(self):
        self.api_conn = 'https://api.data.gov.sg/v1/transport/taxi-availability'
        output_csv_filename = f"taxi_availability_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        # save inside mounted volume or local files dir
        self.output_csv_path = f"/files/data/{output_csv_filename}"

    def read_api(self):
        response = requests.get(self.api_conn)
        if response.status_code == 200:
            print("✅ Successful connection with API.")
            print('-------------------------------')
            self.data = response.json()
        elif response.status_code == 404:
            raise Exception("❌ Unable to reach URL.")
        else:
            raise Exception(f"❌ API request failed: {response.status_code}")

    def extract(self):
        self.df = json_normalize(self.data)

    def transform(self):
        df = self.df

        # rename top-level CRS info
        rename_main = {
            'type': 'type',
            'features': 'features',
            'crs.type': 'crs_type',
            'crs.properties.href': 'crs_properties_href',
            'crs.properties.type': 'crs_properties_type'
        }
        df.rename(columns=rename_main, inplace=True)

        # expand nested "features"
        df_feature = json_normalize(self.data, 'features')

        rename_feature = {
            'type': 'feature_type',
            'geometry.type': 'geometry_type',
            'geometry.coordinates': 'geometry_coordinates',
            'properties.timestamp': 'properties_timestamp',
            'properties.taxi_count': 'properties_taxi_count',
            'properties.api_info.status': 'properties_api_info_status'
        }
        df_feature.rename(columns=rename_feature, inplace=True)

        # merge top-level and feature-level data
        df_merged = df.assign(key=1).merge(df_feature.assign(key=1), on='key').drop('key', axis=1)

        # ensure coordinates are stringified for CSV
        df_merged['geometry_coordinates'] = df_merged['geometry_coordinates'].astype(str)
        df_merged.drop(['features'], axis=1, inplace=True)

        self.df = df_merged

    def write_to_csv(self):
        os.makedirs(os.path.dirname(self.output_csv_path), exist_ok=True)
        self.df.to_csv(self.output_csv_path, index=False)
        print(f"✅ Data successfully written to CSV: {self.output_csv_path}")


def run_taxi_api_to_csv():
    etl = TaxiReadApi()
    etl.read_api()
    etl.extract()
    etl.transform()
    etl.write_to_csv()
