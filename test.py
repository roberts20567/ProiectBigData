#IMPORTS AND MISC
import os, sys
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#END IMPORTS AND MISC



# Create a SparkSession
spark = SparkSession.builder.appName("PySpark data aquisition").getOrCreate()

#Fetching csv files from github, saving and loading them using SparkFile

url = "https://raw.githubusercontent.com/umbrae/reddit-top-2.5-million/refs/heads/master/data/3DS.csv"
spark.sparkContext.addFile(url)

df = spark.read.csv("file:///"+SparkFiles.get("3DS.csv"), header=True, inferSchema= True, multiLine=True)

df.show()