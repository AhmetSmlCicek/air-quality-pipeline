import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import *
import os
from dotenv import load_dotenv
from pyspark.sql.functions import *
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import date
from azure.storage.blob import BlobServiceClient,BlobClient
from prefect import task

#custom log configuration
message_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
my_logger = logging.getLogger("__stream__")
my_logger.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler("stream_{:%Y-%m-%d}.log".format(date.today()),when="midnight")
file_handler.setFormatter(message_formatter)

my_logger.addHandler(file_handler)


#Create Blob service client to list file names to process
load_dotenv()
azure_storage_key = os.getenv("azure_storage_key")
conn_string = os.getenv("azure_conn_string")
blob_service_client = BlobServiceClient.from_connection_string(conn_string)
container_client = blob_service_client.get_container_client("air-data")


#Note: To connect Azure Blob storage and read files via Spark, we have to download 'hadoop-azure' and 'azure-storage' jars.
#Apart from these two dependencies, I had to download jetty-util jar as well following error I had.
#Always make sure that the version of all these dependencies match your hadoop version
builder = SparkSession.builder \
                    .master("local[*]") \
                    .appName('jsonstream.com') \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("fs.azure.account.key.myprojectdata.blob.core.windows.net",azure_storage_key) \
                    .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.AzureLogStore")
            
                    
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



#We define the schema we want from json file
schema = StructType([
    StructField("Concentration",MapType(StringType(),StringType(),True)),
    StructField("AQI",MapType(StringType(),StringType()),True),
    StructField("ReadTime",StringType(),True)
])


def dataStream_in_deltaTable():
    """ Read each json file in Blob container and write relavant data into Delta table.
    We read files one by one because, files contain air quality data for different station. However,
    files do not contain station Id. We get station id from file name for each station, that's why
    reading file by file is necessary."""

    # get files name in s3 Bucket and read each json file into dataframe
    for file in container_client.list_blob_names():
        try:
            df = spark.read.schema(schema).format("json").load(f"wasbs://air-data@myprojectdata.blob.core.windows.net/{file}") \
                      .withColumn("station_id",regexp_extract(input_file_name(),r'net/(.*?)\.json',1))
            #save raw data in bronze delta table
            df.write.format("delta").mode("append").save("wasbs://delta-table@myprojectdata.blob.core.windows.net/air-quality-bronze")
            #extract relevant data from nested dictionary and save it into silver delta table
            df_delta = df.withColumn("PM10",df.AQI.getItem("PM10")) \
                .withColumn("SO2",df.AQI.getItem("SO2")) \
                .withColumn("CO",df.AQI.getItem("CO")) \
                .withColumn("O3",df.AQI.getItem("O3")) \
                .withColumn("AQI_Index",df.AQI.getItem("AQIIndex")) \
                .drop("AQI") \
                .drop("Concentration")
            df_delta.write.format("delta").mode("append").save("wasbs://delta-table@myprojectdata.blob.core.windows.net/air-quality-silver")
            #record succesful data save in the log file
            my_logger.info(f"Data of {file} written in delta table")
        
        except:
            #keep track of any unsucessful air quality data write operation
            my_logger.info(f"Data of {file} not written in delta table")

    #upload log file log container
    with open("stream_{:%Y-%m-%d}.log".format(date.today()),"rb") as log_file:
        blob = BlobClient.from_connection_string(conn_str=conn_string, container_name="log-data", blob_name="stream_{:%Y-%m-%d}.log".format(date.today()))
        blob.upload_blob(log_file)



if __name__=="__main__":
    dataStream_in_deltaTable()

