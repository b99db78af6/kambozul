from googleapiclient.discovery import build
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import db_config
import logging
import sys

# Get current date / time
teraz = datetime.now()

# Logging config
logging.basicConfig(filename=f'logs/{teraz}_youtube_channels.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

# db parameters
user = db_config.user
pwd = db_config.pwd
db_host = db_config.db_host
db_port = db_config.db_port
db_name = db_config.db_name

# Replace 'YOUR_API_KEY' with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'
channel_handle = 'popaswpieprz'
# channel_handle = 'raportzakcji'
# channel_handle = 'MotoBieda'

kanaly = []

# kanaly.append('popaswpieprz')
# kanaly.append('raportzakcji')
# kanaly.append('CaroSeria')
# kanaly.append('CeBeCeBe')
# kanaly.append('PatrykMikiciuk')
# kanaly.append('Motokuncik')
#kanaly.append('TERENWIZJA')
# kanaly.append('StrzeleckiVideo')
#kanaly.append('profesorchris')
# kanaly.append('ANKS')
# kanaly.append('MotoBieda')
# kanaly.append('coobcio')
#kanaly.append('barturban')
# kanaly.append('woczykijwAzji')
kanaly.append('PatrykPiatekVlog')
print(kanaly)



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
    
def get_channel_details(channel_handle, api_key):
    logging.info(f'Processing channel: {channel_handle}')
    # Build the YouTube API client
    youtube = build('youtube', 'v3', developerKey=api_key)

    # Get the channel ID using the channel handle
    response = youtube.search().list(
        part='snippet',
        q=channel_handle,
        type='channel'
    ).execute()

    if 'items' not in response or not response['items']:
        logging.warning(f'Channel: {channel_handle} not found!')
        return "Channel not found"

    channel_id = response['items'][0]['snippet']['channelId']

    # Get channel details using the channel ID
    channel_response = youtube.channels().list(
        part='snippet,statistics,contentDetails,brandingSettings',
        id=channel_id
    ).execute()

    if 'items' not in channel_response or not channel_response['items']:
        logging.warning(f'Channel details for: {channel_handle} not found!')
        return "Channel details not found"

    channel = channel_response['items'][0]

    channel_details = {
        'name': channel['snippet']['title'],
        'description': channel['snippet']['description'],
        'location': channel['snippet'].get('country', 'Not Provided'),
        'channel_open_date': channel['snippet']['publishedAt'],
        'view_count': channel['statistics']['viewCount'],
        'subscriber_count': channel['statistics'].get('subscriberCount', 'Hidden'),
        'video_count': channel['statistics']['videoCount'],
        'keywords': channel['brandingSettings']['channel'].get('keywords', 'Not Provided')
    }

    return channel_details

# Get the channel ID for the given handle
for channel_handle in kanaly:
    channel_id = get_channel_id_by_handle(channel_handle)

    if channel_id:
        print(f"The channel ID for handle '@{channel_handle}' is: {channel_id}")
    else:
        print(f"Channel with handle '@{channel_handle}' not found.")

    #channel_details = get_channel_details(get_channel_id_by_handle(channel_handle), api_key)
    channel_details = get_channel_details(channel_id, api_key)

    print(channel_details)

    # Create a DataFrame from the video details
    df = pd.DataFrame(pd.json_normalize(channel_details))

    df['channel_id'] = channel_id

    # Add a column with the current timestamp as data refresh date
    df['data_refresh_date'] = teraz

    # reorder
    df = df[['channel_id', 'name', 'description', 'location', 'channel_open_date', 'view_count', 'video_count', 'keywords', 'data_refresh_date']]

    print(df)

    # alternatively save stuff into PostgresDB
    engine = create_engine(f'postgresql://{user}:{pwd}@{db_host}:{db_port}/{db_name}')
    df.to_sql('load_yt_channels', engine, if_exists='append', index=False)