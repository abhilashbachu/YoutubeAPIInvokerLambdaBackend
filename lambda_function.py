import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from urllib.parse import urlparse, parse_qs
import datetime
import time
import boto3

def lambda_handler(event, context):
    # Set the API key
    api_key = "AIzaSyBOxR6evmLJIYBK7QIa5u8uvch01EDx5XA"
    # Build the YouTube API client
    youtube = build('youtube', 'v3', developerKey=api_key)
    # event_data = json.loads(event['body'])
    # url = event_data["YouTubeURL"]
    url="https://www.youtube.com/watch?v=rnPsW5BC1xI"
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    param_value = query_params.get('v', None)
    api_key = "AIzaSyBOxR6evmLJIYBK7QIa5u8uvch01EDx5XA"
    res=liveStreamOrNot(youtube,param_value[0],api_key)
    idt=1;
    for ind in res:
        sendToKinesis(ind,idt)
        idt=idt+1
    return {
        'statusCode': 200,
        'body': json.dumps(res)
    }

def liveStreamOrNot(youtube,video_id,api_key):
    listres=[]
    response = youtube.videos().list(
        part='liveStreamingDetails',
        id=video_id,
        key=api_key
    ).execute()
    if 'items' in response:
        if 'liveStreamingDetails' in response['items'][0]:
            print('Video is a live chat')
            live_chat_id = response['items'][0]['liveStreamingDetails']['activeLiveChatId']
            return liveChatObjects(youtube,live_chat_id,api_key)
        else:
            return video_commentsby_id(video_id)

def liveChatObjects(youtube,live_chat_id,api_key):
    res=[]
    val=0;
    time.sleep(60)
    while True:
        response = get_latest_chat_messages(youtube,live_chat_id,api_key)
        val=val+1
        #.append(response)
        for item in response['items']:
            res.append(item)
        break
    return res
    
def get_latest_chat_messages(youtube,live_chat_id,api_key):
    response = youtube.liveChatMessages().list(
        liveChatId=live_chat_id,
        part='snippet',
        maxResults=2000,
        key=api_key
        ).execute()
    return response
        
def video_commentsby_id(video_id):
	# empty list for storing reply
	replies = []
	api_key = "AIzaSyBOxR6evmLJIYBK7QIa5u8uvch01EDx5XA"

	# creating youtube resource object
	youtube = build('youtube', 'v3',
					developerKey=api_key)

	# retrieve youtube video results
	video_response=youtube.commentThreads().list(
	    part='snippet,replies',
	    maxResults=2000,
	    videoId=video_id
	    ).execute()
	outloop =1
	# iterate video response
	while video_response:
		for item in video_response['items']:
			replies.append(item)
		if 'nextPageToken' in video_response:
			print("iam here token thing")
			next_page_token = video_response['nextPageToken']
			video_response=youtube.commentThreads().list(
				part='snippet,replies',
				maxResults=2000,
				videoId=video_id,
				pageToken = next_page_token
			).execute()
		else:
			video_response = 'None'
			break
		
	return replies;
	
def sendToKinesis(res,idt):
    print("Printing things")
    print(res)
    stream_name = 'YouTubeDataKinesis'
    k_client = boto3.client('kinesis', region_name='us-east-2')
    put_response= k_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(res),
        PartitionKey='YouTubeDataKinesis'+str(idt))
    print(put_response)