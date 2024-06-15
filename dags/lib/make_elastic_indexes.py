import os
import re
import sys

from pyspark.sql import SQLContext, SparkSession
import convert_data
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.environ["HADOOP_HOME"] = "E:\\Apps\\hadoop-2.8.3"

cwd = os.getcwd()  # Get the current working directory (cwd)
cwd = cwd.replace("\\", "/")
cwd = cwd + "/../datalake/"
DATALAKE_ROOT_FOLDER = cwd


def make_elastic_indexes(path):
    spark = SparkSession \
        .builder \
        .appName("Convert To Elastic Indexes") \
        .config("es.index.auto.create", "true") \
        .config("es.nodes.wan.only", "true") \
        .config("spark.jars", "C:\\Users\\aaron\\.ivy2\\jars\\org.elasticsearch_elasticsearch-spark-30_2.12-8.6.1.jar") \
        .getOrCreate()
    df = spark.read.load(path)
    df.registerTempTable("data")
    print(df.show())
    df.write.format("org.elasticsearch.spark.sql") \
     .option("es.nodes.wan.only", "true") \
     .option('es.nodes', 'https://dep1-2160db.es.us-central1.gcp.cloud.es.io') \
     .option('es.port', 9243).option('es.resource', 'stint_data').option("es.net.http.auth.user", "elastic") \
     .option("es.net.http.auth.pass", "FMAZQHbivP3B4ZxZJhDm6ajL") \
     .save()

make_elastic_indexes(DATALAKE_ROOT_FOLDER + "/usage/stints")
