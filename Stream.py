#!/usr/bin/python3
import json
import boto3
import pandas as pd
import numpy as np


client = boto3.client('firehose')

def SendData(data):

    json_file = data
    observations = json.loads(json_file)
    response = client.put_record(
    DeliveryStreamName = 'StreamName'
    Record = {
        'Data':json.dumps(observations)
        }
    )    
    
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("SUCCESS, your request ID is :"+response["ResponseMetadata"]["RequestId"])

    else:
        print("ERROR : something went wrong")
        exit(1)

def formatData(df):
    formatedData = df.to_json()
    SendData(formatedData)

def htmlToDf(data):
    df = pd.read_html(data)
    df = df[0]
    df.columns = df.iloc[0]
    formatData(df)

def excelToDf(data):
    df = pd.read_excel(data)
    formatData(df)

def csvToDf(data):
    df = pd.read_csv(data)
    formatData(df)

def jsonToDf(data):
    formatData(data)

def fileTypeHandler(url):

    if url.endswith('.html'):
        htmlToDf(url)

    elif url.endswith('.xls','.xlsx'):
        excelToDf(url)
    
    elif url.endswith('.csv'):
        csvToDf(url)

    elif url.endswith('.json'):
        jsonToDf(url)
    
    else:
        htmlToDf(url)

fileTypeHandler("Some URL")
