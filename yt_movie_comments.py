from googleapiclient.discovery import build
import os

# Replace with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'

# YouTube video ID for which you want to retrieve comments
video_id = 'hFF5FGUGoZA'

youtube = build('youtube', 'v3', developerKey=api_key)

def get_video_comments(video_id):
    comments = []
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100,  # Adjust maxResults as per your needs (1-100)
        textFormat='plainText'
    )
    
    while request:
        response = request.execute()
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)
        
        request = youtube.commentThreads().list_next(request, response)
    
    return comments

# Get comments for the specified video
video_comments = get_video_comments(video_id)

# Print each comment
for index, comment in enumerate(video_comments, start=1):
    print(f"Comment {index}: {comment}")

# Optionally, you can save the comments to a file
# with open('video_comments.txt', 'w', encoding='utf-8') as f:
#     f.write('\n'.join(video_comments))
