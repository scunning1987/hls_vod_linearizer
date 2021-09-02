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

    # create exceptions list to capture all exceptions throughout function
    exceptions = []
    exceptions.clear()

    def createItem(db,item):
        LOGGER.info("Running DB Put Item function, item primary key is : %s " % (asset_name))
        ## Put Item
        response = db_client.put_item(TableName=db,Item=item)
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
            exceptions.append("Unable to get object from S3, got exception: %s " % (e))


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

    item_for_content_library = {
        "assetlocation": {
            "S": asset_url
        },
        "assetname": {
            "S": asset_name
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

    if duration == 0:
        LOGGER.error("Error getting asset information or parsing manifest correctly")
        raise Exception("Error getting asset information or parsing manifest correctly")
    else:
        LOGGER.info("Creating a new DB Item for asset")
        response = createItem(db_contentlibrary,item_for_content_library)


    # Now make sure this new asset is playing out from now
    # 1. Check Content Management DB to see if there is currently an asset playing
    # IF FOUND : Recreate the currently playing asset to end in epoch time now, then currently playing asset
    #            Create new asset playing now until end of time
    # IF NOT FOUND: Create new asset playing now until end of time


    try:
        get_item_response = db_client.get_item(TableName=db_contentmanagement,Key={"endtimeepoch":{"N":"999999999999"}})
        LOGGER.info("Got existing now Playing item...")
        LOGGER.debug("Response from current playing item check : %s " % (get_item_response))
    except Exception as e:
        exceptions.append("Unable to get item from DB, got exception : %s " % (e))
        get_item_response = ""
        LOGGER.debug("Unable to get item from DB, got exception : %s " % (e))

    if "Item" in get_item_response:
        nowPlaying = get_item_response['Item']
        nowPlayingPrimaryKey = nowPlaying['endtimeepoch']['N']

        nowPlaying['endtimeepoch']['N'] = str(int(datetime.datetime.utcnow().strftime('%s')))
        nowPlayingToLast = nowPlaying

        try:
            # delete item 999999998
            delete_item_response = db_client.delete_item(TableName=db_contentmanagement,Key={"endtimeepoch":{"N":"999999999999"}})
            LOGGER.info("Current now playing item has been deleted")
        except Exception as e:
            LOGGER.error("Unable to delete current now playing item, got exception: %s " % (e))
            exceptions.append("Unable to delete current now playing item, got exception: %s " % (e))

        try:
            createItem(db_contentmanagement,nowPlayingToLast)
            LOGGER.info("Creating a new schedule item for the previous asset")
        except Exception as e:
            LOGGER.error("Unable to create new schedule entry for the old now playing item, got exception: %s " % (e))
            exceptions.append("Unable to create new schedule entry for the old now playing item, got exception: %s " % (e))


    endtime = "999999999999"

    newPlayingItem = {
        "endtimeepoch": {
            "N": endtime
        },
        "assetname": {
            "S": asset_name
        },
        "segments": {
            "N": segments
        },
        "assetlocation": {
            "S": asset_url
        },
        "duration": {
            "N": duration
        },
        "genre": {
            "S": "demo"
        }
    }

    try:
        newItemResponse = createItem(db_contentmanagement,newPlayingItem)
        LOGGER.info("Created new DB item in schedule for asset : %s " % (asset_name))
        LOGGER.debug("Response for creating new now playing item : %s " % (newItemResponse))
    except Exception as e:
        LOGGER.error("Unable to create new playlist item: %s " % (e))
        raise Exception("Unable to create new playlist item: %s " % (e))

    return {
        "status":"COMPLETE",
        "exceptions": exceptions
    }