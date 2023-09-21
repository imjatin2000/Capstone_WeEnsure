# Databricks notebook source
account_name="weensureadls"
container_name = "batchcontainer"
storage_acc_key = "PEZvTff5mPF6upQkAAXFbmeSJSlSZHqzPbcgz7z5SRBzhUi1bUuvLBfsOa37M1meufUzgjZFRDoD+ASt1YNNeA=="

#dbutils.fs.mount(
#source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
#mount_point = "/mnt/batchdata/",
 # extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_acc_key}
  #)

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
from io import BytesIO

# COMMAND ----------

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/d5106d1e-f9f5-4eb6-b9ac-d28573cdca9d_83d04ac6-cb74-4a96-a06a-e0d5442aa126_HealthCareZip.zip"


local_zip_file_path = "/dbfs/mnt/batchdata/data/data.zip"
local_unzip_dir = "/dbfs/mnt/batchdata/data/unzipped_data"
 
subprocess.run(["wget", "-O", local_zip_file_path, zip_file_url], check=True)
subprocess.run(["unzip", "-q", local_zip_file_path, "-d", local_unzip_dir], check=True)

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=weensureadls;AccountKey=PEZvTff5mPF6upQkAAXFbmeSJSlSZHqzPbcgz7z5SRBzhUi1bUuvLBfsOa37M1meufUzgjZFRDoD+ASt1YNNeA=="

container_name = "batchcontainer"

folder_name = "unzipped"  


blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_files = os.listdir(local_unzip_dir)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    blob_path = os.path.join(folder_name, file_name)

    blob_client = container_client.get_blob_client(blob_path)

    with open(local_file_path, "rb") as data:

        blob_client.upload_blob(data)

 

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")

 

os.remove(local_zip_file_path)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    os.remove(local_file_path)

print("Cleaned up local files")

# COMMAND ----------


