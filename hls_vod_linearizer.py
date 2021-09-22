'''
Copyright (c) 2021 Scott Cunningham

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Summary: This script is designed to create a linear HLS playlist using VOD content

Original Author: Scott Cunningham
'''

import json
import logging
import boto3
import time
import datetime
import uuid
import os
import math

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

'''
DynamoDB database names
00001_ContentLibrary
00001_ContentManagement
00001_Clients
'''

def lambda_handler(event, context):

    LOGGER.info("Printing Event:")
    LOGGER.info(event)
    db_client = boto3.client('dynamodb')
    sliding_window = int(os.environ['SLIDING_WINDOW'])
    cdn_base_url = os.environ['CDN']

    # initialize s3 boto cliennt
    s3_client = boto3.client('s3')

    ## Functions ## START ##

    # Function to get DB Item that needs playing
    def dbGetItemToPlay(table_name,):
        LOGGER.debug("Doing a call to Dynamo to get asset name that should be playing")
        try:
            response = db_client.scan(TableName=table_name)
        except Exception as e:
            exceptions.append("error getting data from DynamoDB, got exception: %s" %  (e))
            return e
        return response


    # Function to create an item in the schedule
    def dbCreateScheduledProgram(asset,type):
        # type : immediate | append
        LOGGER.debug("Doing a call to Dynamo to create scheduled item in list")

    # Function to get client info from database
    def dbGetClientInfo(clients_db,client_id):
        LOGGER.debug("Doing a call to Dynamo to get client information")
        try:
            get_item_response = db_client.get_item(TableName=clients_db,Key={"client_id":{"S":client_id}})
        except Exception as e:
            exceptions.append("#EXT-X-STATUS: UNABLE TO GET CLIENT INFO FROM DATABASE, GOT EXCEPTION %s" % (str(e).upper()))
            return "#EXT-X-STATUS: UNABLE TO GET CLIENT INFO FROM DATABASE, GOT EXCEPTION %s" % (str(e).upper())
        return get_item_response

    # Function to create an item in the db for client tracking
    def dbCreateClient(clients_db,client_id,requesttime_epoch):
        LOGGER.debug("Doing a call to Dynamo to create client info")
        try:
            create_item_response = db_client.put_item(TableName=clients_db,Item={"client_id":{"S":client_id},"session_start":{"N":str(requesttime_epoch)}})
        except Exception as e:
            exceptions.append("#EXT-X-STATUS: UNABLE TO REGISTER CLIENT IN DATABASE, GOT EXCEPTION %s" % (str(e).upper()))
            return "#EXT-X-STATUS: UNABLE TO REGISTER CLIENT IN DATABASE, GOT EXCEPTION %s" % (str(e).upper())
        return create_item_response

    # Function to get the playlist returned
    def dbGetList(startepoch,endepoch):
        LOGGER.debug("Doing a call to Dynamo to get portion on playlist returned")
        # db.scan with filters and conditions

    # New master manifest builder
    def master_manifest_constructor(assetinfo,client_id):
        asseturl = assetinfo['AssetLocation']
        asset_bucket = asseturl.split("/")[2]
        asset_key = '/'.join(asseturl.split("/")[3:])
        path_to_object = asseturl.rsplit("/",1)[0] + "/"

        try:
            LOGGER.debug("Getting object from S3 : %s" % (path_to_object))
            response = s3_client.get_object(Bucket=asset_bucket,Key=asset_key)
        except Exception as e:
            LOGGER.error("Unable to get object from S3, got exception: %s " % (e))
            exceptions.append("Unable to get object from S3, got exception: %s " % (e))
            return errorOut("#EXT-X-STATUS: ERROR - UNABLE TO GET MASTER MANIFEST FROM ORIGIN")

        master_manifest_original = response['Body'].read().decode('utf-8')

        rendition_list = []
        rendition_number = 0
        for line in master_manifest_original.split('\n'):
            if ".m3u8" in line:
                master_manifest_original = master_manifest_original.replace(line,"%s/%s.m3u8" % (channel_name,rendition_number))
                rendition_list.append(path_to_object+line)
                rendition_number += 1
        master_manifest_client_id = master_manifest_original.replace(".m3u8",".m3u8?client_id=%s" % (client_id))
        return {"master_manifest_client_id":master_manifest_client_id,"rendition_list":rendition_list}



    def manifestLinearizer(schedule,time_window_start,time_window_end,rendition_number):
        LOGGER.debug("Performing manifest stitching")
        # session_start_epoch
        # sliding_window
        media_sequence = 1

        assetstartepoch = session_start_epoch
        total_loops_for_discontinuity_sequence = 0

        manifest_constructor = dict()
        manifest_constructor['media_sequence'] = media_sequence
        manifest_constructor['discontinuity_sequence'] = total_loops_for_discontinuity_sequence
        manifest_constructor_segments = []


        for item in schedule:
            endtimeepoch = int(item['EndTimeEpoch'])
            segments = int(item['AssetSegments'])
            duration = int(item['AssetDuration'])
            asseturl = item['AssetLocation']
            nowplaying = item['NowPlaying']

            if endtimeepoch > session_start_epoch:
                #
                # this captures all files that have been in the schedule since session start...
                #

                # Get manifest
                # Get master manifest
                master_manifest = master_manifest_constructor(item,"xxx")
                child_full_url =  master_manifest['rendition_list'][rendition_number]

                asset_bucket = child_full_url.split("/")[2]
                asset_key = '/'.join(child_full_url.split("/")[3:])
                path_to_segments = '/'.join(child_full_url.split("/")[3:]).rsplit("/",1)[0] + "/"

                if len(cdn_base_url) > 6:
                    # assume CDN is being used:
                    cdn_no_protocol = cdn_base_url.replace("https://","").replace("http://","")

                    child_url = "https://%s/%s" % (cdn_no_protocol,path_to_segments)

                else:
                    # assume clients can get directly from S3
                    bucket_region = "us-west-2"
                    child_url = "https://%s.s3.%s.amazonaws.com/%s" % (asset_bucket,bucket_region,path_to_segments)


                try:
                    LOGGER.debug("Getting object from S3: %s " % (asset_key))
                    response = s3_client.get_object(Bucket=asset_bucket,Key=asset_key)
                except Exception as e:
                    LOGGER.error("Unable to get object from S3, got exception: %s " % (e))
                    exceptions.append("Unable to get object from S3, got exception: %s " % (e))
                    return errorOut("#EXT-X-STATUS: ERROR - UNABLE TO GET MANIFEST FROM ORIGIN")

                childmanifestraw = response['Body'].read().decode('utf-8')
                child_headers_list = childmanifestraw.split("#")[0:4]
                #for manifest_line in child_manifest_list:


                ###
                ###
                ###

                def manifest_iterator(manifest_start_seconds,manifest_end_seconds,segments,oldest_loop):

                    manifest_start = manifest_start_seconds
                    manifest_end = manifest_end_seconds

                    segment_index = []
                    segment_index.clear()
                    extinfstart = []
                    extinfstart.clear()
                    LOGGER.debug("segment index : " +str(len(segment_index)))
                    manifest_timeline = 0

                    for index, line in enumerate(childmanifestraw.split("#")):
                        if 'EXTINF' in line:
                            extinfstart.append(index)
                            manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])

                            if manifest_timeline >= manifest_start and manifest_timeline <= manifest_end:
                                segment_index.append(index)
                                if segments:
                                    manifest_constructor_segments.append("#"+line)

                    if len(segment_index) == 0:
                        segments_into_manifest = 0
                        starting_segment = 0
                    else:
                        segments_into_manifest = segment_index[-1] - int(extinfstart[0])
                        starting_segment = segment_index[0] - int(extinfstart[0])

                    if oldest_loop:
                        manifest_constructor['media_sequence'] += starting_segment
                ###
                ###
                ###


                if endtimeepoch < time_window_start:
                    LOGGER.debug("Asset finished and is no longer in playlist : %s " % (asseturl))

                    epoch_end = endtimeepoch
                    loops_to_epoch_end = math.floor((epoch_end - assetstartepoch) / duration)
                    manifest_constructor['discontinuity_sequence'] = manifest_constructor['discontinuity_sequence'] + loops_to_epoch_end

                    starting_media_sequence_of_last_loop = loops_to_epoch_end * segments
                    media_sequence += starting_media_sequence_of_last_loop
                    manifest_constructor['media_sequence'] = media_sequence

                    manifest_end = epoch_end - assetstartepoch - (loops_to_epoch_end * duration)
                    manifest_start = 0

                    manifest_iterator(manifest_start,manifest_end,False,False)


                if endtimeepoch > time_window_start:
                    LOGGER.info("getting segments from this asset : %s " % (asseturl))

                    epoch_start = requesttime_epoch - sliding_window

                    if epoch_start < assetstartepoch:
                        epoch_start = assetstartepoch
                        #loops_to_epoch_start = 0

                    if requesttime_epoch < endtimeepoch:
                        epoch_end = requesttime_epoch
                    else:
                        epoch_end = endtimeepoch
                    loops_to_epoch_start = math.floor((epoch_start - assetstartepoch) / duration)
                    if loops_to_epoch_start < 0:
                        loops_to_epoch_start = 0

                    loops_to_epoch_end = math.floor((epoch_end - assetstartepoch) / duration)
                    manifest_constructor['discontinuity_sequence'] += loops_to_epoch_start
                    starting_media_sequence_of_oldest_loop = loops_to_epoch_start * segments
                    loops_of_asset = math.ceil((epoch_end - epoch_start) / duration)


                    manifest_constructor['media_sequence'] += starting_media_sequence_of_oldest_loop

                    manifest_start_epoch = epoch_start

                    manifest_start = epoch_start - assetstartepoch - (loops_to_epoch_start * duration)

                    if len(manifest_constructor_segments) > 0:
                        manifest_constructor_segments.append("#EXT-X-DISCONTINUITY")


                    oldest_loop = True
                    looper = dict()
                    for i in range(loops_of_asset,-1,-1):
                        if i == 0:
                            manifest_end = epoch_end - assetstartepoch - (loops_to_epoch_end * duration)
                        else:
                            manifest_end = duration


                        LOGGER.debug("loop iteration : %s , manifest_start : %s , manifest_end : %s " % (str(i),str(manifest_start),str(manifest_end)) )

                        looper[str(i)] = {
                            "start":manifest_start,
                            "end":manifest_end,
                            "duration":manifest_end-manifest_start,
                            "epoch": epoch_start + manifest_start

                        }

                        manifest_iterator(manifest_start,manifest_end,True,oldest_loop)
                        LOGGER.debug("manifest_start:%s,manifest_end:%s" % (manifest_start,manifest_end))

                        #oldest_loop = False

                        if i > 0:
                            manifest_start = 0
                            manifest_constructor_segments.append("#EXT-X-DISCONTINUITY")
                        oldest_loop = False
                    LOGGER.warning(looper)
                assetstartepoch = endtimeepoch


        if manifest_constructor_segments[-1] == "#EXT-X-DISCONTINUITY":
            manifest_constructor_segments.pop(-1)

        manifest_constructor['discontinuity_sequence'] = manifest_constructor['discontinuity_sequence'] + manifest_constructor_segments.count("#EXT-X-DISCONTINUITY")
        manifest_constructor['segments'] = manifest_constructor_segments

        #return len(manifest_constructor['segments']) - manifest_constructor['segments'].count("#EXT-X-DISCONTINUITY")

        #media_sequence = manifest_constructor['media_sequence'] - len(manifest_constructor['segments']) + manifest_constructor['segments'].count("#EXT-X-DISCONTINUITY")
        media_sequence = manifest_constructor['media_sequence']
        ## Construct manifest


        ## HEADERS
        child_headers = '#'.join(child_headers_list)
        child_headers += "#EXT-X-MEDIA-SEQUENCE:"+str(media_sequence)+"\n"
        if manifest_constructor['discontinuity_sequence'] > 0:
            child_headers += "#EXT-X-DISCONTINUITY-SEQUENCE:%s\n" % (str(manifest_constructor['discontinuity_sequence']))

        ## SEGMENTS in Manifest

        new_child_manifest = []
        for line in manifest_constructor['segments']:
            if "EXTINF" in line:
                new_child_manifest.append(line.replace("\n",str(media_sequence)+"\n"+str(child_url),1))
                media_sequence += 1
            else:
                new_child_manifest.append(line+"\n")



        # Stitch headers + old and new manifest
        child_manifest = child_headers + ''.join(new_child_manifest)

        return child_manifest



    def errorOut(message):
        response_status = {
            "statusCode": 404,
            "headers": {
                "Content-Type": "application/vnd.apple.mpegURL",
                "Access-Control-Allow-Origin":"*"
            },
            "body": message
        }
        return response_status


    def nowPlaying(content_management_db,session_start_epoch):
        '''
        This function will make a call to DynamoDB to get the asset that should be playing right now.
        '''
        ## Get Items from DB
        getItemToPlayResponse = dbGetItemToPlay(content_management_db)

        if len(exceptions) > 0:
            return errorOut("#EXT-X-STATUS: %s" % (exceptions))

        ## Find which item should be playing right now
        listOfPlaybackItems = []
        currentAndFutureItems = dict()
        for item in getItemToPlayResponse['Items']:
            endtimeepoch = int(item['endtimeepoch']['N'])
            assetlocation = item['assetlocation']['S']
            assetduration = item['duration']['N']
            assetsegments = item['segments']['N']

            if endtimeepoch > session_start_epoch: #or endtimeepoch > requesttime_epoch - sliding_window:
                currentAndFutureItems[endtimeepoch] = {"AssetLocation":assetlocation,"AssetDuration":assetduration,"EndTimeEpoch":endtimeepoch,"AssetSegments":assetsegments,"NowPlaying":"False"}
                listOfPlaybackItems.append(endtimeepoch)

        # sort the list to see which endtimeepoch is closest - that will determine which item should be playing now
        listOfPlaybackItems.sort()
        currentItemEndTimeEpoch = 0

        for endtime in listOfPlaybackItems:
            if endtime > requesttime_epoch:
                if currentItemEndTimeEpoch == 0:
                    currentItemEndTimeEpoch = endtime

        # Iterate through 'currentAndFutureItems' dictionary to get asset name corresponding to endtimeepoch. There should only ever be 2 items to iterate through here
        currentPlaybackWindow = []
        for item in currentAndFutureItems:
            assetlocation = currentAndFutureItems[item]
            if item == currentItemEndTimeEpoch:
                assetlocation['NowPlaying'] = "True"
                currentPlaybackWindow.append(assetlocation)
            else:
                currentPlaybackWindow.append(assetlocation)

        LOGGER.debug("Current Playback Item is : %s" % (currentPlaybackWindow))
        return currentPlaybackWindow


    ### Creating global variables - START

    exceptions = []
    exceptions.clear()

    requesttime_iso8601 = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    requesttime_epoch = int(datetime.datetime.utcnow().strftime('%s'))
    #requesttime_epoch = 1631849400
    ### Creating global variables - END

    # check request length first
    request_path_to_list = event['path'].split("/")

    rendition_name = ""
    if len(request_path_to_list) < 3 or len(request_path_to_list) > 4:
        return errorOut("#EXT-X-STATUS: MALFORMED REQUEST, NOT VALID")

    if len(request_path_to_list) == 4:
        rendition_name = request_path_to_list[3]

    # this should be the enterprise customer id
    enterprise_customer_id = request_path_to_list[1]

    # this should be the channel name
    channel_name = request_path_to_list[2].split(".m3u8")[0]

    content_library_db = "%s_ContentLibrary" % (enterprise_customer_id)
    content_management_db = "%s_ContentManagement" % (enterprise_customer_id)
    clients_db = "%s_Clients" % (enterprise_customer_id)

    # check if client is known
    try:
        client_id = event['queryStringParameters']['client_id']
        new_client = False
        LOGGER.debug("Known client requesting manifest, id : %s" %(client_id))
    except:
        if len(request_path_to_list) == 4:
            return errorOut("#EXT-X-STATUS: CANNOT PROCESS REQUEST, CLIENT ID NOT SENT OR NOT KNOWN BY SYSTEM")
        new_client = True
        client_id = uuid.uuid4().hex
        LOGGER.debug("New client connected, generating a unique ID for playback tracking, id : %s " % (client_id))


    if len(request_path_to_list) == 3: # This is a request for master manifest

        # Get master manifest from current playing asset
        # use requesttime_epoch to check content_management_db
        now_and_future_playing = nowPlaying(content_management_db,requesttime_epoch)

        now_and_future_playing = sorted(now_and_future_playing, key=lambda k: k['EndTimeEpoch'])

        # Get Master manifest and populate query strings
        if now_and_future_playing[0]['EndTimeEpoch'] < requesttime_epoch:
            master_manifest = master_manifest_constructor(now_and_future_playing[1],client_id)
        else:
            master_manifest = master_manifest_constructor(now_and_future_playing[0],client_id)

        # If Client is new, create DB entry for client
        if new_client:
            create_client_id = dbCreateClient(clients_db,client_id,requesttime_epoch)

            redirect_url = "https://%s%s?client_id=%s" % (event['requestContext']['domainName'],event['requestContext']['path'],client_id)

            return {
                "statusCode": 301,
                "headers": {
                    "Content-Type": "application/vnd.apple.mpegURL",
                    "Access-Control-Allow-Origin":"*",
                    "Location":redirect_url
                }
            }

            if len(exceptions) > 0:
                return errorOut(exceptions)

        # Return master manifest back to client
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/vnd.apple.mpegURL",
                "Access-Control-Allow-Origin":"*"
            },
            "body": master_manifest['master_manifest_client_id']
        }

    elif len(request_path_to_list) == 4: # this is a request for the child playlist
        try:
            client_id = event['queryStringParameters']['client_id']
        except:
            return errorOut("#EXT-X-STATUS: YOU MUST REQUEST A RENDITION PLAYLIST WITH A VALID CLIENT ID - PLEASE TRY AGAIN")

        # Rendition number being requested

        try:
            rendition_string = request_path_to_list[3].split(".m3u8")[0]
            rendition_number = int(rendition_string)
        except:
            return errorOut("#EXT-X-STATUS: YOU ARE REQUESTING AN INVALID RENDITION")
        # use requesttime_epoch to check content_management_db

        # get session start time
        session_start_epoch = dbGetClientInfo(clients_db,client_id)
        if len(exceptions) > 0:
            return errorOut("#EXT-X-STATUS: UNABLE TO GET CLIENT DATA BACK FROM DB")

        session_start_epoch = int(session_start_epoch['Item']['session_start']['N'])
        schedule_since_session_start = nowPlaying(content_management_db,session_start_epoch)
        schedule_since_session_start = sorted(schedule_since_session_start, key=lambda k: k['EndTimeEpoch'])


        # get the epoch time window for the manifest
        time_window_end = int(requesttime_epoch)
        time_window_start = time_window_end - sliding_window

        new_child_manifest = manifestLinearizer(schedule_since_session_start,time_window_start,time_window_end,rendition_number)

        # child playlist to grab
        # Where we are in asset : requesttime_epoch - session_start_epoch - math.floor(requesttime_epoch - session_start_epoch / now_and_future_playing[0]['AssetDuration']) * now_and_future_playing[0]['AssetDuration']
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/vnd.apple.mpegURL",
                "Access-Control-Allow-Origin":"*"
            },
            "body": new_child_manifest
        }