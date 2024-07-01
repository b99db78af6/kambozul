from googleapiclient.discovery import build

# Replace 'YOUR_API_KEY' with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'
channel_handle = 'popaswpieprz'

youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_id_by_handle(handle):
    request = youtube.search().list(
        part='snippet',
        type='channel',
        q=handle
    )
    response = request.execute()
    
    if 'items' in response and len(response['items']) > 0:
        channel_id = response['items'][0]['snippet']['channelId']
        return channel_id
    else:
        return None

# Get the channel ID for the given handle
channel_id = get_channel_id_by_handle(channel_handle)

if channel_id:
    print(f"The channel ID for handle '@{channel_handle}' is: {channel_id}")
else:
    print(f"Channel with handle '@{channel_handle}' not found.")