from googleapiclient.discovery import build
import os

# Replace with your actual API key
api_key = 'AIzaSyAvVhB5SOn5ufkksrSk6QT7olXYb2ZCmJU'

# YouTube video ID for which you want to retrieve comments
video_id = 'hFF5FGUGoZA'

youtube = build('youtube', 'v3', developerKey=api_key)

# def get_video_comments(video_id):
#     comments = []
#     request = youtube.commentThreads().list(
#         part='snippet',
#         videoId=video_id,
#         maxResults=100,  # Adjust maxResults as per your needs (1-100)
#         textFormat='plainText'
#     )
    
#     while request:
#         response = request.execute()
#         for item in response['items']:
#             comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
#             comments.append(comment)
        
#         request = youtube.commentThreads().list_next(request, response)
    
#     return comments

def get_video_comments(video_id):
    comments = []
    request = youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id,
        maxResults=100,  # Adjust maxResults as per your needs (1-100)
        textFormat='plainText'
    )
    
    while request:
        response = request.execute()
        for item in response['items']:
            comment_info = {
                'comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'published_at': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                'comment_id': item['snippet']['topLevelComment']['id'],
                'reply_count': item['snippet']['totalReplyCount'],
                'like_count': item['snippet']['topLevelComment']['snippet']['likeCount'],
                'parent_id': None  # Top-level comments do not have a parent ID
            }
            comments.append(comment_info)
            
            # If the comment has replies, fetch them as well
            if item['snippet']['totalReplyCount'] > 0:
                replies = item.get('replies', {}).get('comments', [])
                for reply in replies:
                    reply_info = {
                        'comment_text': reply['snippet']['textDisplay'],
                        'author': reply['snippet']['authorDisplayName'],
                        'published_at': reply['snippet']['publishedAt'],
                        'comment_id': reply['id'],
                        'reply_count': 0,  # Replies do not have replies in this context
                        'like_count': reply['snippet']['likeCount'],
                        'parent_id': item['snippet']['topLevelComment']['id']  # ID of the parent comment
                    }
                    comments.append(reply_info)
        
        request = youtube.commentThreads().list_next(request, response)
    
    return comments

# Get comments for the specified video
video_comments = get_video_comments(video_id)

# Print each comment
for index, comment in enumerate(video_comments, start=1):
    print(f"Comment {index}: {comment_text}")

# Optionally, you can save the comments to a file
# with open('video_comments.txt', 'w', encoding='utf-8') as f:
#     f.write('\n'.join(video_comments))
