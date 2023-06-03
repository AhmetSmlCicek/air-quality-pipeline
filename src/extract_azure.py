import os
from dotenv import load_dotenv
import requests
from azure.storage.blob import BlobClient, BlobServiceClient
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import date,timedelta
import json
from prefect import task

#Create connection to AWS S3 bucket
load_dotenv()
conn_string = os.getenv("azure_conn_string")

#get yestarday timeframe to retrive data from API
yesterday = date.today() - timedelta(days=1)
start_time = yesterday.strftime('%d.%m.%Y 00:00:00')
end_time = yesterday.strftime('%d.%m.%Y 23:59:59')

#custom log configuration
message_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
my_logger = logging.getLogger("__extract__")
my_logger.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler("extract_{:%Y-%m-%d}.log".format(date.today()),when="midnight")
file_handler.setFormatter(message_formatter)

my_logger.addHandler(file_handler)



def get_data():
    """Retrive data from API of Istanbul municipality.
    Station id, start and end time must be specified to retrive air quality data which
    is being measured daily across the city."""
    
    #get station ids 
    with open ("air_stationIDs.json","r") as c_file:
        air_stations = json.load(c_file)

    #get airquality data and save in JSON file. Then upload the file into S3
    try:
        for station in air_stations:
            with open (f"air-data/{station}_{yesterday}.json","w") as file:
                    air_ist_by_Station = f"https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId?StationId={station}&StartDate={start_time}&EndDate={end_time}"
                    air_data = requests.get(air_ist_by_Station)
                    json.dump(air_data.json(),file)
            my_logger.info(f"Airdata of {yesterday} for station {station} has been extracted")            
    except:
        my_logger.error(f"Airdata of {yesterday} for station {station} couldnt been extracted")


def uploadFile_to_azure():
    
    """ Upload air data as json file into Azure Blob storage"""

    for file in os.listdir("air-data"):
        try:
            blob = BlobClient.from_connection_string(conn_str=conn_string, container_name="air-data", blob_name=file)
            with open(f"air-data/{file}","rb") as json_data:
                blob.upload_blob(json_data)
                my_logger.info(f"{file} has been uploaded to Blob") 
                #remove json files once they are uploaded
                #os.remove(file)           
        except:
            my_logger.info(f"{file} not uploaded to Blob")
            raise Exception("File not uploaded")
         
    
    #upload log file log container
    with open("extract_{:%Y-%m-%d}.log".format(date.today()),"rb") as log_file:
        blob = BlobClient.from_connection_string(conn_str=conn_string, container_name="log-data", blob_name="extract_{:%Y-%m-%d}.log".format(date.today()))
        blob.upload_blob(log_file)

if __name__=="__main__":
     get_data()
     uploadFile_to_azure()