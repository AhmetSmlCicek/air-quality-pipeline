# Air-quality-pipeline
This repository contains a data pipeline that extracts air quality data from the API of the Istanbul municipality, processes it, and stores it in Azure Blob Storage. The pipeline utilizes Apache Spark for data streaming and transformation, Delta Lake for data storage, and Prefect for workflow orchestration.

# Overview
The data pipeline follows the following steps:

Extraction: Air quality data is retrieved from the API of the Istanbul municipality's Air Quality Monitoring Stations.

Storage: The extracted data for each station is saved in JSON files, which are stored in an Azure Blob container.

Streaming: The data is streamed from the Azure Blob container using Apache Spark and saved in a "bronze" Delta table, preserving the raw data.

Transformation: The raw data from the bronze Delta table is transformed and saved in a "silver" Delta table, enabling further analysis and exploration.

Workflow Orchestration: Prefect is used to schedule and orchestrate the daily execution of the extraction, streaming, and transformation processes.

Visualization: Power BI is connected to the silver Delta table in Azure Blob Storage to explore and visualize the air quality data.

# Pipeline Components
The repository consists of the following components:

extract.py: Python script responsible for extracting air quality data from the Istanbul municipality API and saving it as JSON files in the Azure Blob container.

stream.py: Apache Spark script that streams data from the Azure Blob container and saves it in the bronze Delta table. Raw data is then transformed with Spark and saved into the silver Delta table.

main.py: Prefect workflow definition that schedules and orchestrates the daily execution of the extraction, streaming, and transformation processes.

README.md: This file, providing an overview and instructions for the data pipeline.

# Setup and Configuration
To set up and configure the data pipeline, follow these steps:

Clone the repository: git clone https://github.com/your-username/air-quality-data-pipeline.git.

Install the required dependencies for Python, Apache Spark, Delta Lake, and Prefect as specified in the requirements.txt file.

Configure the necessary credentials and connection settings for Azure Blob Storage, including the storage account name, access key, and container details.

Update the configuration files for the extraction, streaming, and transformation scripts to reflect the correct API endpoints, file paths, and Azure Blob Storage details.

Configure the Prefect workflow in main.py to set the desired schedule and any other workflow-related configurations.

To deploy Prefect workflow, run the following command: prefect deployment apply main_flow-deployment.yaml

To learn how to configure deployment and use cloud storage for your deployment please refer to https://docs.prefect.io/2.10.12/concepts/deployments/ 

# Usage
To use the data pipeline, follow these steps:

Run the main.py script using Prefect to schedule and orchestrate the daily execution of the extraction, streaming, and transformation processes.

The extract.py script will run daily and fetch air quality data from the Istanbul municipality API, saving it as JSON files in the Azure Blob container.

The stream.py script, executed by the workflow, will stream the JSON files from the Azure Blob container using Apache Spark and store the raw data in the bronze Delta table. Once the raw data is stored in the bronze Delta table, Spark will perform the necessary transformations and save the processed data in the silver Delta table.

Connect Power BI to the silver Delta table stored in Azure Blob Storage to explore and visualize the air quality data.