'''
This script will calculate media duration and number of segments from an https origin
'''
import json
import boto3
import datetime
import math
import os
from botocore.vendored import requests

def lambda_handler(event, context):

    # manual method for testing purposes. This should be CloudWatch event driven after successful ingest into MediaPackage
    assets_to_ingest = [
        'https://e4276182346b9b6a5929cba3cb0886df.egress.mediapackage-vod.us-west-2.amazonaws.com/out/v1/c7c4eb122fc34c41b25d3b6e4e493d89/f03bf4fca60b403a83344c8bf85b4cca/4727555785ba442cb60d1d555b960546/index.m3u8',
        'https://e4276182346b9b6a5929cba3cb0886df.egress.mediapackage-vod.us-west-2.amazonaws.com/out/v1/726a7ffc554b4b03aa964b5657a6b38e/f03bf4fca60b403a83344c8bf85b4cca/4727555785ba442cb60d1d555b960546/index.m3u8',
        'https://e4276182346b9b6a5929cba3cb0886df.egress.mediapackage-vod.us-west-2.amazonaws.com/out/v1/c06ec96ff4884b5d85dfc3ea7f370cc0/f03bf4fca60b403a83344c8bf85b4cca/4727555785ba442cb60d1d555b960546/index.m3u8'
    ]

    db_name = os.environ['CONTENT_MANAGEMENT_DB']

    client = boto3.client('dynamodb')

    def getTables():
        ## List DynamoDB Tables
        response = client.list_tables(
            Limit=10
        )
        return response

    ## Get Items
    def getItems():
        response = client.scan(TableName=db_name)
        return response

    def createItem(name, location, duration, segments):
        ## Put Item
        response = client.put_item(
            TableName=db_name,
            Item={
                "assetlocation": {
                    "S": location
                },
                "assetname": {
                    "S": name
                },
                "duration": {
                    "S": duration
                },
                "segments": {
                    "S": segments
                },
                "genre": {
                    "S": "demo"
                }
            }
        )
        return response

    ##
    def durationCalculator(asseturl):
        #
        # get asset from s3 master manifest
        #

        urlonly = asseturl.rsplit('/', 1)[0] + "/"
        childlist = []

        # sending get request and saving the response as response object 
        mastermanifest = requests.get(url = asseturl)

        for line in mastermanifest.text.split('\n'):
            if ".m3u8" in line:
                childlist.append(urlonly+line)

        #
        # get child manifest / playlist and work out total duration
        #  
        childmanifestraw = requests.get(url = str(childlist[0]))
        duration = 0
        segments = 0
        filedetails = dict()
        for line in childmanifestraw.text.split("#"):
            if 'EXTINF' in line:
                segments = segments + 1
                duration = duration + float(line.split(",")[0].split(":")[1])
        duration = [{'duration': duration}]
        segments = [{'segments': segments}]
        filedetails['duration'] = duration
        filedetails['segments'] = segments
        print(filedetails)
        return filedetails

    for asseturl in assets_to_ingest:

        assetname = asseturl.rsplit("/",1)[-1].rsplit('.', 1)[0]

        assetname = 'slate_60'
        asseturl = 'https://e4276182346b9b6a5929cba3cb0886df.egress.mediapackage-vod.us-west-2.amazonaws.com/out/v1/c06ec96ff4884b5d85dfc3ea7f370cc0/f03bf4fca60b403a83344c8bf85b4cca/4727555785ba442cb60d1d555b960546/index.m3u8'


        filedetails = durationCalculator(asseturl)
        duration = str(filedetails['duration'][0]['duration'])
        segments = str(filedetails['segments'][0]['segments'])

        if duration == 0:
            return "error getting asset information"
        else:
            response = createItem(assetname, asseturl, duration, segments)
            return response