import os
import sys

import pandas as pd

def list_files(dir):
    r = []
    for root, dirs, files in os.walk(dir):
        for name in files:
            r.append(os.path.join(root, name).replace('\\', '/'))
    return r

def convert_data(files):
    for f in files:
        TARGET_PATH = f.replace(f.split('/')[-1], "").replace('raw', 'formatted')
        if not os.path.exists(TARGET_PATH):
            os.makedirs(TARGET_PATH)
        if f.endswith('.csv'):
            table = pd.read_csv(f)
            driverName = TARGET_PATH.split("/")[-2]
            table['driverId'] = driverName
            table.to_parquet(TARGET_PATH + f.split('/')[-1].replace('csv', 'parquet'))
        else:
            table = pd.read_json(f)
            table.to_parquet(TARGET_PATH + f.split('/')[-1].replace('json', 'parquet'))

convert_data(list_files('../datalake/raw/lapData'))