import os
from pyspark.sql import SQLContext
import convert_data
from pyspark import SparkContext
import re

cwd = os.getcwd()  # Get the current working directory (cwd)
cwd = cwd.replace("\\", "/")
cwd = cwd + "/../datalake/"
DATALAKE_ROOT_FOLDER = cwd


def combine_data(files):
    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
    curr_pits_path = None
    df_pits = None
    for f in files:
        OUTPUT_FOLDER = f.replace("formatted/lapData", "usage").replace(f.split('/')[-1], "")
        driverName = OUTPUT_FOLDER.split("/")[-2]
        OUTPUT_FOLDER = OUTPUT_FOLDER.replace(driverName + "/", "")
        teamName = OUTPUT_FOLDER.split("/")[-2]
        PITS_PATH = (f.replace("lapData", "pitData").replace(teamName + "/", "").replace(driverName + "/", "")
                     .replace("laps.", "pits."))
        OUTPUT_FOLDER = OUTPUT_FOLDER.replace(teamName + "/", "")
        df_laps = sqlContext.read.parquet(f)
        df_laps.registerTempTable("laps")
        if curr_pits_path is not PITS_PATH:
            df_pits = sqlContext.read.parquet(PITS_PATH)
            df_pits.registerTempTable("pits")
        print(df_laps.show())
        print(df_pits.show())

combine_data(convert_data.list_files(DATALAKE_ROOT_FOLDER + "formatted/lapData/"))
