from flask import Flask, request, send_file
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
from dateutil import parser
import csv
import isodate
import sqlite3
import logging
import io

app = Flask(__name__)

# Logging config
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

# go to http://127.0.0.1:5000/get_channel_data?channel_id=UCyFrUC936RTrwRjE0tEbZCQ
# Replace 'YOUR_API_KEY' with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'

def get_uploads_playlist_id(youtube, channel_id):
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = request.execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_playlist_id

def get_video_ids(youtube, playlist_id):
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

def get_video_details(youtube, video_ids):
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
                'dislikes': int(item['statistics'].get('dislikeCount', 0)),
                'comments': int(item['statistics'].get('commentCount', 0)),
                'publishedAt': parser.isoparse(item['snippet']['publishedAt']),
                'duration': int(duration)
            })
    return video_details

def get_channel_subscriptions(youtube, channel_id):
    request = youtube.channels().list(
        part='statistics',
        id=channel_id
    )
    response = request.execute()
    subscriber_count = int(response['items'][0]['statistics']['subscriberCount'])
    return subscriber_count

def list_videos_with_views_and_dates(youtube, channel_id):
    uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
    video_ids = get_video_ids(youtube, uploads_playlist_id)
    video_details = get_video_details(youtube, video_ids)
    return video_details

@app.route('/get_channel_data', methods=['GET'])
def get_channel_data():
    channel_id = request.args.get('channel_id')
    if not channel_id:
        return "Error: No channel_id field provided. Please specify a channel_id."

    youtube = build('youtube', 'v3', developerKey=api_key)
    logging.info(f'Processing channel: {channel_id}')

    teraz = datetime.now()

    videos = list_videos_with_views_and_dates(youtube, channel_id)
    df = pd.DataFrame(videos)
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    df['data_refresh_date'] = teraz
    df['channel_id'] = channel_id

    subscriber_count = get_channel_subscriptions(youtube, channel_id)
    df['subscriber_count'] = subscriber_count

    df['likes_views'] = df['likes'] / df['views']
    df['likes_duration'] = df['likes'] / df['duration']
    df['comments_views'] = df['comments'] / df['views']
    df['comments_duration'] = df['comments'] / df['duration']
    df['comments_likes'] = df['comments'] / df['likes']
    df['avg_score'] = (df['likes_views'] + df['likes_duration'] + df['comments_views'] + df['comments_duration'] + df['comments_likes']) / 5
    df['score_x_dur'] = df['avg_score'] * df['duration']
    df = df.sort_values(by='publishedAt', ascending=False)
    df['publishedAt'] = pd.to_datetime(df['publishedAt']).dt.tz_convert(None)
    df['age_in_minutes'] = (df['data_refresh_date'] - df['publishedAt']).dt.total_seconds() / 60
    df['LVR'] = df['likes'] / df['views']
    df['CVR'] = df['comments'] / df['views']
    df['LCR'] = df['likes'] / df['comments']
    df['ER'] = (df['likes'] + df['comments']) / df['views']
    df['DNE'] = (df['likes'] + df['comments']) / df['duration']
    df['Views_per_Subscriber'] = df['views'] / df['subscriber_count']
    df['Likes_per_Subscriber'] = df['likes'] / df['subscriber_count']
    df['Comments_per_Subscriber'] = df['comments'] / df['subscriber_count']
    df['ER_normalized'] = df['ER'] / df['age_in_minutes']

    weights = {
        'ER': 0.3,
        'LVR': 0.15,
        'CVR': 0.15,
        'LCR': 0.1,
        'DNE': 0.1,
        'Views_per_Subscriber': 0.1,
        'ER_normalized': 0.1
    }

    df['Engagement_Score'] = (
        df['ER'] * weights['ER'] +
        df['LVR'] * weights['LVR'] +
        df['CVR'] * weights['CVR'] +
        df['LCR'] * weights['LCR'] +
        df['DNE'] * weights['DNE'] +
        df['Views_per_Subscriber'] * weights['Views_per_Subscriber'] +
        df['ER_normalized'] * weights['ER_normalized']
    )

    df = df[['channel_id', 'subscriber_count', 'data_refresh_date', 'title', 'videoId', 'publishedAt', 'views', 'likes', 'dislikes', 'comments', 'duration', 'likes_views', 'likes_duration', 'comments_views', 'comments_duration', 'comments_likes', 'age_in_minutes', 'avg_score', 'score_x_dur', 'Engagement_Score', 'Views_per_Subscriber', 'Likes_per_Subscriber', 'Comments_per_Subscriber', 'DNE', 'ER', 'ER_normalized']]

    teraz = teraz.strftime("%Y%m%d%H%M")

    # Save the DataFrame to a CSV in-memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
    csv_buffer.seek(0)

    # Return the CSV file as a response
    return send_file(csv_buffer, as_attachment=True, download_name=f'{teraz}_youtube_videos.csv', mimetype='text/csv')

if __name__ == '__main__':
    app.run(debug=True)
