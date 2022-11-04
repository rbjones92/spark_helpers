# Robert Jones
# 7.22.122
# Print all distinct schemas from DF, and show what is in list


import os
import sys
from pyspark.sql import SparkSession, conf
from pyspark.sql import Window as W
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, DoubleType, IntegerType, DateType, StringType, FloatType, LongType, BinaryType

# directory = 'C:/Users/Robert.Jones/Desktop/Springboard/Capstone/data_pipeline/nyc_data/trip_data/'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


directory = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Desktop/Springboard/Capstone/data_pipeline/nyc_data/trip_data/' 


spark = SparkSession.builder.getOrCreate()

# Grab files for each different type of ridershare

for_hire = []
green_taxi = []
high_volume = []
yellow_taxi = []


for file in os.listdir(directory):
    if file.startswith('For-Hire'):
        for_hire.append(file)
    if file.startswith('Green Taxi'):
        green_taxi.append(file)
    if file.startswith('High Volume'):
        high_volume.append(file)
    if file.startswith(('Yellow Taxi','yellow')):
        yellow_taxi.append(file)

all_data = [for_hire,green_taxi,high_volume,yellow_taxi]

print(all_data)



def get_schema(data):

    file_list = []
    schema_list = []

    for files in data:
        df = spark.read.format('parquet').load(directory+files)
        file_list.append(files)
        schema_list.append(str(df.dtypes))


    list_zip = zip(file_list,schema_list)
    zipped_list = list(list_zip)


    df_schema = StructType([ \
        StructField("File",StringType(),True), \
        StructField("Schema",StringType(),True),
    ]) 

    df = spark.createDataFrame(zipped_list,schema= df_schema)

    df = df.groupBy("Schema").agg(F.collect_list('File'))

    data_str = data[0]
    name = data_str.split(' ')[0]

    # df.write.json(f'C:/Users/Robert.Jones/Desktop/Springboard/Capstone/data_pipeline/nyc_data/meta_data/schemas/{name}_schema.json')

    df.show()



for data in all_data:
    get_schema(data)
    

