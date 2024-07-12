from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
from dateutil import parser
import csv
import isodate  # To parse the ISO 8601 duration
import sqlite3
import logging
import sys

# Get current date / time
teraz = datetime.now()

# Logging config
logging.basicConfig(filename=f'logs/{teraz}_youtube_videos.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

# Replace 'YOUR_API_KEY' with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'
#channel_id = 'UCyFrUC936RTrwRjE0tEbZCQ' # gapa
#channel_id = 'UC1v8Pb3mVVhctpado1zkY-Q' # raport z akacji
#channel_id = 'UCVwO3pAsl8u88yeHX9zPGqA' # cebe czeczen

channel_id = sys.argv[1]
print(channel_id)

logging.info(f'Processing channel: {channel_id}')

youtube = build('youtube', 'v3', developerKey=api_key)

# Get the uploads playlist ID
def get_uploads_playlist_id(channel_id):
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = request.execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_playlist_id

# Get video IDs from the uploads playlist
def get_video_ids(playlist_id):
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return video_ids

# Get video details including view count and publish date
def get_video_details(video_ids):
    video_details = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()
        for item in response['items']:
            duration = isodate.parse_duration(item['contentDetails']['duration']).total_seconds()
            video_details.append({
                'title': item['snippet']['title'],
                'videoId': item['id'],
                'views': int(item['statistics'].get('viewCount', 0)),
                'likes': int(item['statistics'].get('likeCount', 0)),
                'dislikes': int(item['statistics'].get('dislikeCount', 0)),  # This may return 0 or be omitted
                'comments': int(item['statistics'].get('commentCount', 0)),
                'publishedAt': parser.isoparse(item['snippet']['publishedAt']),
                'duration': int(duration)
            })
    return video_details

# Main function to list videos with view counts and publish dates
def list_videos_with_views_and_dates(channel_id):
    uploads_playlist_id = get_uploads_playlist_id(channel_id)
    video_ids = get_video_ids(uploads_playlist_id)
    video_details = get_video_details(video_ids)
    return video_details

# Get the number of channel subscriptions
def get_channel_subscriptions(channel_id):
    request = youtube.channels().list(
        part='statistics',
        id=channel_id
    )
    response = request.execute()
    subscriber_count = int(response['items'][0]['statistics']['subscriberCount'])
    return subscriber_count

# Get video details for the specified channel
videos = list_videos_with_views_and_dates(channel_id)

# Create a DataFrame from the video details
df = pd.DataFrame(videos)

# Convert the 'publishedAt' column to datetime
df['publishedAt'] = pd.to_datetime(df['publishedAt'])

# Add a column with the current timestamp as data refresh date
df['data_refresh_date'] = teraz

# Add a column with the channel ID
df['channel_id'] = channel_id

# Get the number of channel subscriptions
subscriber_count = get_channel_subscriptions(channel_id)

# Add a column with the number of channel subscriptions
df['subscriber_count'] = subscriber_count

# Calculate metrics like/view	like/dur	coments/views	comments/dur	comments/likes
df['likes_views'] = df['likes'] / df['views']
df['likes_duration'] = df['likes'] / df['duration']
df['comments_views'] = df['comments'] / df['views']
df['comments_duration'] = df['comments'] / df['duration']
df['comments_likes'] = df['comments'] / df['likes']

# my own metrics
df['avg_score'] = (df['likes_views'] + df['likes_duration'] +df['comments_views'] + df['comments_duration'] + df['comments_likes']) / 5
df['score_x_dur'] = df['avg_score'] * df['duration']

# Sort the DataFrame by published date in descending order
df = df.sort_values(by='publishedAt', ascending=False)

# Convert the 'publishedAt' column to datetime and make it timezone-naive
df['publishedAt'] = pd.to_datetime(df['publishedAt']).dt.tz_convert(None)

# Calculate the time difference (in minutes) between data_refresh_date and publishedAt
df['age_in_minutes'] = (df['data_refresh_date'] - df['publishedAt']).dt.total_seconds() / 60


# Calculate individual measures
df['LVR'] = df['likes'] / df['views']
df['CVR'] = df['comments'] / df['views']
df['LCR'] = df['likes'] / df['comments']
df['ER'] = (df['likes'] + df['comments']) / df['views']
df['DNE'] = (df['likes'] + df['comments']) / df['duration']

# New measures incorporating subscribers and age of video
df['Views_per_Subscriber'] = df['views'] / df['subscriber_count']
df['Likes_per_Subscriber'] = df['likes'] / df['subscriber_count']
df['Comments_per_Subscriber'] = df['comments'] / df['subscriber_count']
df['ER_normalized'] = df['ER'] / df['age_in_minutes']

# Define weights for each measure including the new ones
weights = {
    'ER': 0.3,
    'LVR': 0.15,
    'CVR': 0.15,
    'LCR': 0.1,
    'DNE': 0.1,
    'Views_per_Subscriber': 0.1,
    'ER_normalized': 0.1
}

# Calculate Engagement Score with new measures
df['Engagement_Score'] = (
    df['ER'] * weights['ER'] +
    df['LVR'] * weights['LVR'] +
    df['CVR'] * weights['CVR'] +
    df['LCR'] * weights['LCR'] +
    df['DNE'] * weights['DNE'] +
    df['Views_per_Subscriber'] * weights['Views_per_Subscriber'] +
    df['ER_normalized'] * weights['ER_normalized']
)

# Reorder columns to move 'channel_id' and 'data_refresh_date' to the front
df = df[['channel_id', 'subscriber_count', 'data_refresh_date', 'title', 'videoId', 'publishedAt', 'views', 'likes', 'dislikes', 'comments', 'duration', 'likes_views', 'likes_duration', 'comments_views', 'comments_duration', 'comments_likes', 'age_in_minutes', 'avg_score', 'score_x_dur', 'Engagement_Score', 'Views_per_Subscriber', 'Likes_per_Subscriber', 'Comments_per_Subscriber', 'DNE', 'ER', 'ER_normalized']]

# Print the DataFrame
print(df)

# Format the date and time as a string in the desired format
teraz = teraz.strftime("%Y%m%d%H%M")

# Optionally, save the DataFrame to a CSV file
df.to_csv(f'{teraz}_youtube_videos.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Save the DataFrame to a SQLite database
conn = sqlite3.connect('youtube_videos.db')
# df.to_sql('videos', conn, if_exists='replace', index=False)
df.to_sql('videos', conn, if_exists='append', index=False)
conn.close()