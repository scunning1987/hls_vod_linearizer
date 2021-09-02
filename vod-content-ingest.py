'''
This script will calculate media duration and number of segments of an asset from S3
'''
import json
import boto3
import datetime
import math
import os
from botocore.vendored import requests
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):

    LOGGER.info("Received event : %s " % (event))

    db_contentmanagement = os.environ['CONTENT_MANAGEMENT_DB']
    db_contentlibrary = os.environ['CONTENT_LIBRARY_DB']

    LOGGER.info("Content management database : %s " % (db_contentmanagement))
    LOGGER.info("Content library database %s " % (db_contentlibrary))

    # Initialize AWS service boto3 clients#
    db_client = boto3.client('dynamodb')
    s3_client = boto3.client('s3')


    def createItem(name, location, duration, segments):
        LOGGER.info("Running DB Put Item function, item primary key is : %s " % (name))
        ## Put Item
        response = db_client.put_item(
            TableName=db_contentlibrary,
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
    def durationCalculator(asset_bucket,asset_playlist_key):
        LOGGER.info("Running durationCalculator function")
        #
        # get child manifest / playlist and work out total duration
        #

        try:
            LOGGER.info("Getting object from S3")
            response = s3_client.get_object(Bucket=asset_bucket,Key=asset_playlist_key)
        except Exception as e:
            LOGGER.error("Unable to get object from S3, got exception: %s " % (e))


        asset_playlist_manifest = response['Body'].read().decode('utf-8')
        LOGGER.debug("Child playlist contents : %s " % (asset_playlist_manifest))

        duration = 0
        segments = 0
        filedetails = dict()

        LOGGER.info("Iterating through manifest to get segmennt count and duration")
        for line in asset_playlist_manifest.split("#"):
            if 'EXTINF' in line:
                segments = segments + 1
                duration = duration + float(line.split(",")[0].split(":")[1])
        duration = [{'duration': duration}]
        segments = [{'segments': segments}]
        filedetails['duration'] = duration
        filedetails['segments'] = segments

        LOGGER.info("Completed analysis of playlist: %s " % (filedetails))
        return filedetails



    # Pull event json outputgroupdetails dict to variable
    outputGroups = event['detail']['outputGroupDetails']
    LOGGER.debug("MediaConvert complete event - output group details : %s " % (outputGroups))

    # iterate through outputs (to account for multiple output groups). Assume first hit HLS_GROUP is what we want to use
    for outputGroup in outputGroups:

        # pull type to variable
        type = outputGroup['type']

        # If type is HLS_GROUP then capture the URL details and analyze length
        if type == "HLS_GROUP":
            LOGGER.info("Found HLS Group in event output")
            # HLS S3 URI
            asset_url = outputGroup['playlistFilePaths'][0]

            # iterate through playlist files and get s3 uri for a video rendition
            outputGroupDetails = outputGroup['outputDetails']

            for outputGroupDetail in outputGroupDetails:
                if "videoDetails" in outputGroupDetail:
                    asset_playlist_url = outputGroupDetail['outputFilePaths'][0]

    asset_name = asset_url.split("/")[-1].replace(".m3u8","")
    LOGGER.info("Asset Name: %s " % (asset_name))

    # parse asset_playlist_url to bucket and s3 key path
    asset_bucket = asset_playlist_url.split("/")[2]
    asset_playlist_key = '/'.join(asset_playlist_url.split("/")[3:])
    LOGGER.info("Asset bucket: %s " % (asset_bucket))
    LOGGER.info("Asset playlist key: %s " % (asset_playlist_key))

    filedetails = durationCalculator(asset_bucket,asset_playlist_key)

    duration = str(filedetails['duration'][0]['duration'])
    segments = str(filedetails['segments'][0]['segments'])

    if duration == 0:
        LOGGER.error("Error getting asset information or parsing manifest correctly")
        raise Exception("Error getting asset information or parsing manifest correctly")
    else:
        LOGGER.info("Creating a new DB Item for asset")
        response = createItem(asset_name, asset_url, duration, segments)
        return response
