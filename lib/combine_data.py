import os
from pyspark.sql import SQLContext
import convert_data
from pyspark import SparkContext
import re

cwd = os.getcwd()  # Get the current working directory (cwd)
cwd = cwd.replace("\\", "/")
cwd = cwd + "/../datalake/"
DATALAKE_ROOT_FOLDER = cwd


def duration_to_seconds(duration):
    print(duration)
    match = re.match(r'(\d+) days (\d+):(\d+):(\d+\.\d{3})', duration)
    if match:
        days, hours, minutes, seconds = map(float, match.groups())
        total_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
        return total_seconds
    return None


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
        stints = sqlContext.sql(f"SELECT AVG(l.lapTime), Stint, p.driverId, Count(LapNumber), l.Compound FROM laps l "
                                f"INNER JOIN ( Select * from pits pit where pit.driverId = '{driverName}') p "
                                f"ON p.driverId = l.driverId GROUP BY Stint, p.driverId, l.Compound ORDER BY Stint ASC")
        print(df_pits.show())
        print(df_laps.show())
        print(stints.show())


combine_data(convert_data.list_files(DATALAKE_ROOT_FOLDER + "formatted/lapData/"))
