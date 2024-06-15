import os
import re
import sys

from pyspark.sql import SQLContext

from pyspark import SparkContext
from pyspark.sql.functions import lit

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.environ["HADOOP_HOME"] = "E:\\Apps\\hadoop-2.8.3"

cwd = os.getcwd()  # Get the current working directory (cwd)
cwd = cwd.replace("\\", "/")
cwd = cwd + "/../datalake2/"
DATALAKE_ROOT_FOLDER = "../../datalake2/"


def list_files(dir):
    r = []
    for root, dirs, files in os.walk(dir):
        for name in files:
            r.append(os.path.join(root, name).replace('\\', '/'))
    return r

def combine_data():
    sc = SparkContext(appName="CombineData", master="local[*]")
    sqlContext = SQLContext(sc)
    curr_pits_path = None
    df_pits = None
    allData = None
    out = "datalake2/"
    files = list_files(DATALAKE_ROOT_FOLDER + "formatted/lapData/")
    for f in files:
        print(f)
        OUTPUT_FOLDER = f.replace("formatted/lapData", "usage").replace(f.split('/')[-1], "")
        year = re.search(r"(\d{4}\b)", f).group(0)
        driverName = OUTPUT_FOLDER.split("/")[-2]
        OUTPUT_FOLDER = OUTPUT_FOLDER.replace(driverName + "/", "")
        teamName = OUTPUT_FOLDER.split("/")[-2]
        PITS_PATH = (f.replace("lapData", "pitData").replace(teamName + "/", "").replace(driverName + "/", "")
                     .replace("laps.", "pits."))
        OUTPUT_FOLDER = OUTPUT_FOLDER.replace(teamName + "/", "")
        circuitName = OUTPUT_FOLDER.split("/")[-2]
        OUTPUT_FOLDER = OUTPUT_FOLDER.replace(circuitName + "/", "").replace(year + '/', "")
        df_laps = sqlContext.read.parquet(f)
        df_laps.registerTempTable("laps")
        if curr_pits_path is not PITS_PATH:
            df_pits = sqlContext.read.parquet(PITS_PATH)
            df_pits.registerTempTable("pits")
        stints = sqlContext.sql(f"SELECT round(AVG(l.lapTimeInSeconds), 3) as avgLapTime, "
                                f"Stint, p.driverId as driver, Count(LapNumber) as NoOfLaps, l.Compound FROM laps l "
                                f"INNER JOIN ( Select * from pits pit where pit.driverId = '{driverName}') p "
                                f"ON p.driverId = l.driverId GROUP BY Stint, p.driverId, l.Compound ORDER BY Stint ASC")
        stints = (stints.withColumn("team", lit(teamName))
                  .withColumn("year", lit(year))
                  .withColumn("circuit", lit(circuitName)))
        stints.write.save(DATALAKE_ROOT_FOLDER + "usage/" + "stints", mode="append")

combine_data()

