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
import requests
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

    def master_manifest_constructor(assetinfo,client_id):
        asseturl = assetinfo['AssetLocation']
        master_manifest_get_response = requests.get(url = asseturl)
        if master_manifest_get_response.status_code != 200:
            return errorOut("#EXT-X-STATUS: ERROR - UNABLE TO GET MASTER MANIFEST FROM ORIGIN")

        master_manifest_original = master_manifest_get_response.text

        urlonly = asseturl.rsplit('/', 1)[0] + "/"
        rendition_list = []
        rendition_number = 0
        for line in master_manifest_original.split('\n'):
            if ".m3u8" in line:
                master_manifest_original = master_manifest_original.replace(line,"%s/%s.m3u8" % (channel_name,rendition_number))
                rendition_list.append(urlonly+line)
                rendition_number += 1
        master_manifest_client_id = master_manifest_original.replace(".m3u8",".m3u8?client_id=%s" % (client_id))
        return {"master_manifest_client_id":master_manifest_client_id,"rendition_list":rendition_list}

    def manifestLinearizer(schedule,time_window_start,time_window_end,rendition_number):
        LOGGER.debug("Performing manifest stitching")
        # session_start_epoch
        # sliding_window
        media_sequence = 1

        assetstartepoch = session_start_epoch - 18
        total_loops = 0
        older_child_manifest = []
        older_child_manifest.clear()
        new_child_manifest = []
        old_child_manifest = []
        older_manifest_loop = False

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

                if endtimeepoch < time_window_start:

                    LOGGER.debug("This is a scheduled item that has long since lapsed. need to calculate Media Sequence")
                    # determine how many loops have fully completed, then pull child to get 'number of segments' into the final loop to finalize the MediaSequence calculation

                    # This is an asset we're looping and may loop for a while
                    loops_since_asset_start = math.floor((endtimeepoch - assetstartepoch) / duration)
                    starting_media_sequence_of_last_loop = loops_since_asset_start * segments
                    seconds_into_current_loop = endtimeepoch - assetstartepoch - (loops_since_asset_start * duration)
                    manifest_start = 0
                    manifest_end = seconds_into_current_loop
                    segment_index = []
                    segment_index.clear()
                    extinfstart = []
                    extinfstart.clear()

                    # Get master manifest
                    master_manifest = master_manifest_constructor(item,"xxx")
                    child_full_url =  master_manifest['rendition_list'][rendition_number]
                    child_url = child_full_url.rsplit('/', 1)[0] + "/"

                    # Get child manifest
                    childmanifestraw = requests.get(url = child_full_url)

                    manifest_timeline = 0
                    for index, line in enumerate(childmanifestraw.text.split("#")):
                        if 'EXTINF' in line:
                            extinfstart.append(index)
                            manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])

                            if manifest_timeline >= manifest_start and manifest_timeline <= manifest_end:
                                segment_index.append(index)

                    if len(segment_index) == 0:
                        segments_into_manifest = 0
                    else:
                        segments_into_manifest = segment_index[-1] - int(extinfstart[0])

                    media_sequence += starting_media_sequence_of_last_loop + segments_into_manifest

                    assetstartepoch = endtimeepoch
                    total_loops += loops_since_asset_start+1

                if endtimeepoch > time_window_start and endtimeepoch <= time_window_end:

                    LOGGER.debug("This is a scheduled item that we are transitioning from")
                    # need to create older_child_manifest
                    # need to calculate MEDIA-SEQUENCE-here - probably the same way as "old_child_manifest" calculates its MediaSequence

                    # This is an asset we're looping and may loop for a while
                    loops_since_asset_start = math.floor((endtimeepoch - assetstartepoch) / duration)

                    starting_media_sequence_of_last_loop = loops_since_asset_start * segments

                    seconds_into_current_loop = endtimeepoch - assetstartepoch - (loops_since_asset_start * duration)

                    manifest_end = seconds_into_current_loop
                    manifest_start = requesttime_epoch - assetstartepoch - (loops_since_asset_start * duration) - sliding_window
                    segment_index = []
                    segment_index.clear()
                    extinfstart = []
                    extinfstart.clear()
                    #return manifest_start

                    # Get master manifest
                    master_manifest = master_manifest_constructor(item,"xxx")
                    child_full_url =  master_manifest['rendition_list'][rendition_number]
                    child_url = child_full_url.rsplit('/', 1)[0] + "/"

                    # Get child manifest
                    childmanifestraw = requests.get(url = child_full_url)

                    manifest_timeline = 0
                    media_sequence += starting_media_sequence_of_last_loop

                    ###
                    older_child_manifest.clear()
                    if manifest_start < 0: # we were in the middle of a loop on the previous asset. Need to make sure we preserve the older loop
                        manifest_timeline = 0
                        old_loop_start = duration + manifest_start
                        old_loop_end = duration
                        extinfstart.clear()

                        for index, line in enumerate(childmanifestraw.text.split("#")):
                            if 'EXTINF' in line:
                                extinfstart.append(index)
                                manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])


                                if manifest_timeline >= old_loop_start and manifest_timeline <= old_loop_end:
                                    older_child_manifest.append("#"+line.replace("\n",str(media_sequence+int(index)-int(extinfstart[0])-segments)+"\n"+str(child_url),1))


                    if len(older_child_manifest) > 0:
                        older_manifest_loop = True
                        older_child_manifest.append("#EXT-X-DISCONTINUITY\n")


                    manifest_timeline = 0
                    segment_index.clear()
                    extinfstart.clear()
                    for index, line in enumerate(childmanifestraw.text.split("#")):
                        if 'EXTINF' in line:
                            extinfstart.append(index)
                            manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])
                            if manifest_timeline <= manifest_end:
                                segment_index.append(index)
                            if manifest_timeline >= manifest_start and manifest_timeline <= manifest_end:
                                older_child_manifest.append("#"+line.replace("\n",str(media_sequence+int(index)-int(extinfstart[0]))+"\n"+str(child_url),1))

                    if len(segment_index) == 0:
                        segments_into_manifest = 0
                    else:
                        segments_into_manifest = segment_index[-1] - segment_index[0]

                    media_sequence += segments_into_manifest
                    assetstartepoch = endtimeepoch

                    # This is for the discontinuity Media Sequence counter
                    total_loops += loops_since_asset_start+1

                # this is the current looping asset with no other scheduled item interfering
                #if endtimeepoch > time_window_start and endtimeepoch > time_window_end:
                if nowplaying == "True":

                    # This is an asset we're looping and may loop for a while
                    loops_since_asset_start = math.floor((requesttime_epoch - assetstartepoch) / duration)
                    starting_media_sequence_of_last_loop = loops_since_asset_start * segments

                    seconds_into_current_loop =  requesttime_epoch - assetstartepoch - (loops_since_asset_start * duration)
                    manifest_end = seconds_into_current_loop
                    manifest_start = manifest_end - sliding_window
                    media_sequence = media_sequence + starting_media_sequence_of_last_loop

                    extinfstart = []
                    extinfstart.clear()

                    time_watching = requesttime_epoch - sliding_window - session_start_epoch

                    # Get master manifest
                    master_manifest = master_manifest_constructor(item,"xxx")
                    child_full_url =  master_manifest['rendition_list'][rendition_number]
                    child_url = child_full_url.rsplit('/', 1)[0] + "/"

                    # Get child manifest
                    childmanifestraw = requests.get(url = child_full_url)

                    if manifest_start < 0 and len(older_child_manifest) == 0 and time_watching > 0:

                        media_sequence_header = 0
                        manifest_timeline = 0
                        old_manifest_start = duration + manifest_start
                        old_manifest_end = duration

                        for index, line in enumerate(childmanifestraw.text.split("#")):
                            if 'EXTINF' in line:
                                extinfstart.append(index)
                                manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])

                                if manifest_timeline >= old_manifest_start and manifest_timeline <= old_manifest_end:
                                    old_child_manifest.append("#"+line.replace("\n",str(media_sequence+int(index)-int(extinfstart[0])-segments)+"\n"+str(child_url),1))

                                    if media_sequence_header == 0:
                                        media_sequence_header = media_sequence+int(index)-int(extinfstart[0])-segments

                        manifest_start = 0
                        extinfstart.clear()
                        manifest_timeline = 0
                        for index, line in enumerate(childmanifestraw.text.split("#")):
                            if 'EXTINF' in line:
                                extinfstart.append(index)
                                manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])

                                if manifest_timeline >= manifest_start and manifest_timeline <= manifest_end:
                                    new_child_manifest.append("#"+line.replace("\n",str(media_sequence+int(index)-int(extinfstart[0]))+"\n"+str(child_url),1))



                    else:
                        '''
                        testing
                        '''
                        if requesttime_epoch - assetstartepoch - duration > 0:

                            older_child_manifest.clear()
                            if manifest_start < 0: # we were in the middle of a loop on the previous asset. Need to make sure we preserve the older loop
                                manifest_timeline = 0
                                if duration + manifest_start < 0:
                                    old_loop_start = 0
                                else:
                                    old_loop_start = duration + manifest_start

                                old_loop_end = duration
                                extinfstart.clear()

                                for index, line in enumerate(childmanifestraw.text.split("#")):
                                    if 'EXTINF' in line:
                                        extinfstart.append(index)
                                        manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])


                                        if manifest_timeline >= old_loop_start and manifest_timeline <= old_loop_end:
                                            older_child_manifest.append("#"+line.replace("\n",str(media_sequence+int(index)-int(extinfstart[0])-segments)+"\n"+str(child_url),1))

                        '''
                        testing
                        '''

                        media_sequence_header = 0
                        manifest_timeline = 0
                        extinfstart.clear()
                        manifest_timeline = 0
                        for index, line in enumerate(childmanifestraw.text.split("#")):
                            if 'EXTINF' in line:
                                extinfstart.append(index)
                                manifest_timeline = manifest_timeline + int(line.split(",")[0].split(":")[1])

                                if manifest_timeline >= manifest_start and manifest_timeline <= manifest_end:
                                    new_child_manifest.append("#"+line.replace("\n",str(media_sequence+int(index)-int(extinfstart[0]))+"\n"+str(child_url),1))

                                    if media_sequence_header == 0:
                                        media_sequence_header = media_sequence+int(index)-int(extinfstart[0])
                    ###



                #else:
                #    return errorOut("#EXT-X-STATUS: ERROR STITCHING MANIFEST")

        ## Add a discontinuity if needed
        if len(older_child_manifest) > 0:
            if media_sequence_header == 0: # this is a scenario during a transition and there are no new segments being populated yet
                media_sequence_header = media_sequence
            if older_manifest_loop:
                media_sequence_header += 1
            media_sequence_header -= len(older_child_manifest)
            if len(new_child_manifest) > 0:
                older_child_manifest.append("#EXT-X-DISCONTINUITY\n")
        if len(old_child_manifest) > 0 and len(new_child_manifest) > 0:
            old_child_manifest.append("#EXT-X-DISCONTINUITY\n")

        #media_sequence_header += 1
        ## Configure child headers
        child_headers_list = childmanifestraw.text.split("#")[0:4]
        line_num = 0
        for header in child_headers_list:
            if "DURATION" in header:
                #"EXT-X-TARGETDURATION:7\n"
                dur_with_newline = header.split(":")[1]
                dur = dur_with_newline.split("\n")[0]
                child_headers_list[line_num] = header.replace(dur,str(int(int(dur)-1)))
            line_num += 1

        # join headers together and create MediaSequence header
        child_headers = '#'.join(child_headers_list)
        child_headers += "#EXT-X-MEDIA-SEQUENCE:"+str(media_sequence_header)+"\n"

        # Add Discontinuity Sequence header is loops have occurred
        combined_loops = total_loops + loops_since_asset_start

        if combined_loops > 0:
            child_headers += "#EXT-X-DISCONTINUITY-SEQUENCE:%s\n" % (str(combined_loops))

        # Stitch headers + old and new manifest
        child_manifest = child_headers + ''.join(older_child_manifest) + ''.join(old_child_manifest) + ''.join(new_child_manifest)

        return child_manifest

        '''
        session_start_epoch = int(session_start_epoch['Item']['session_start']['N'])
        #requesttime_epoch
        loops_since_session_start = math.floor((requesttime_epoch - session_start_epoch) / int(now_and_future_playing[now_play]['AssetDuration']))
        starting_media_sequence_of_current_loop = loops_since_session_start * int(now_and_future_playing[now_play]['AssetSegments']
        seconds_into_loop = requesttime_epoch - session_start_epoch - (loops_since_session_start*int(now_and_future_playing[now_play]['AssetDuration']))
        '''


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
    #requesttime_epoch = 1623363467
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