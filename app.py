import csv
import pandas as pd
import datetime
import uuid
from data_source.collector import APICollector
from azure_storage.client import AzureBlobStorage
from fastapi import FastAPI

azure = AzureBlobStorage()
path = 'bbtv'


app = FastAPI()

data = pd.read_csv('files/data.csv', delimiter=',', encoding='utf-8', header=0)
collector = APICollector(azure, data, path).start(data, path)

