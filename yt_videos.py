from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import csv
import isodate  # To parse the ISO 8601 duration

# Replace 'YOUR_API_KEY' with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'
channel_id = 'UCyFrUC936RTrwRjE0tEbZCQ'

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
                'publishedAt': item['snippet']['publishedAt'],
                'duration': int(duration)
            })
    return video_details

# Main function to list videos with view counts and publish dates
def list_videos_with_views_and_dates(channel_id):
    uploads_playlist_id = get_uploads_playlist_id(channel_id)
    video_ids = get_video_ids(uploads_playlist_id)
    video_details = get_video_details(video_ids)
    return video_details

# Get video details for the specified channel
videos = list_videos_with_views_and_dates(channel_id)

# Create a DataFrame from the video details
df = pd.DataFrame(videos)

# Convert the 'publishedAt' column to datetime
df['publishedAt'] = pd.to_datetime(df['publishedAt'])

# Add a column with the current timestamp as data refresh date
df['data_refresh_date'] = datetime.now()

# Add a column with the channel ID
df['channel_id'] = channel_id

# Calculate metrics like/view	like/dur	coments/views	comments/dur	comments/likes
df['likes_views'] = df['likes'] / df['views']
df['likes_duration'] = df['likes'] / df['duration']
df['comments_views'] = df['comments'] / df['views']
df['comments_duration'] = df['comments'] / df['duration']
df['comments_likes'] = df['comments'] / df['likes']

# Sort the DataFrame by published date in descending order
df = df.sort_values(by='publishedAt', ascending=False)

# Reorder columns to move 'channel_id' and 'data_refresh_date' to the front
df = df[['channel_id', 'data_refresh_date', 'title', 'videoId', 'publishedAt', 'views', 'likes', 'dislikes', 'comments', 'duration', 'likes_views', 'likes_duration', 'comments_views', 'comments_duration', 'comments_likes']]

# Print the DataFrame
print(df)

# Optionally, save the DataFrame to a CSV file
df.to_csv('youtube_videos.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)