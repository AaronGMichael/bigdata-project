import os
import sys

import pandas as pd
import re


def list_files(dir):
    r = []
    for root, dirs, files in os.walk(dir):
        for name in files:
            r.append(os.path.join(root, name).replace('\\', '/'))
    return r


def duration_to_seconds(duration):
    try:
        match = re.match(r'(\d+) days (\d+):(\d+):(\d+\.\d{3})', duration)
        if match:
            days, hours, minutes, seconds = map(float, match.groups())
            total_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
            return total_seconds
    except TypeError as e:
        print("TypeError")
    return None


def convert_data(files):
    fileList = []
    for f in files:
        TARGET_PATH = f.replace(f.split('/')[-1], "").replace('raw', 'formatted')
        if not os.path.exists(TARGET_PATH):
            os.makedirs(TARGET_PATH)
        if f.endswith('.csv'):
            table = pd.read_csv(f)
            driverName = TARGET_PATH.split("/")[-2]
            table['driverId'] = driverName
            table["lapTimeInSeconds"] = table.apply(lambda row: duration_to_seconds(row['LapTime']), axis=1)
            table.to_parquet(TARGET_PATH + f.split('/')[-1].replace('csv', 'parquet'))
            fileList.append(TARGET_PATH + f.split('/')[-1].replace('csv', 'parquet'))
        else:
            table = pd.read_json(f)
            table.to_parquet(TARGET_PATH + f.split('/')[-1].replace('json', 'parquet'))
            fileList.append(TARGET_PATH + f.split('/')[-1].replace('json', 'parquet'))
    return fileList


def convert_data_from_root(filePath=""):
    if filePath != "":
        filePath = "/" + filePath
    files = list_files('datalake2/raw'+filePath)
    fileList = []
    for f in files:
        TARGET_PATH = f.replace(f.split('/')[-1], "").replace('raw', 'formatted')
        if not os.path.exists(TARGET_PATH):
            os.makedirs(TARGET_PATH)
        if f.endswith('.csv'):
            table = pd.read_csv(f)
            print(f)
            driverName = TARGET_PATH.split("/")[-2]
            table['driverId'] = driverName
            table["lapTimeInSeconds"] = table.apply(lambda row: duration_to_seconds(row['LapTime']), axis=1)
            table.to_parquet(TARGET_PATH + f.split('/')[-1].replace('csv', 'parquet'))
            fileList.append(TARGET_PATH + f.split('/')[-1].replace('csv', 'parquet'))
        else:
            table = pd.read_json(f)
            table.to_parquet(TARGET_PATH + f.split('/')[-1].replace('json', 'parquet'))
            fileList.append(TARGET_PATH + f.split('/')[-1].replace('json', 'parquet'))
    return fileList

# convert_data(list_files('../datalake/raw/lapData'))
