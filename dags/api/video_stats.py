import requests
import json
from datetime import date
from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlistID():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return playlist_id
    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlistID):
    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistID}&key={API_KEY}"
    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                video_ids.append(item['contentDetails']['videoId'])
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(lst, batch_size):
        for i in range(0, len(lst), batch_size):
            yield lst[i:i + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={','.join(batch)}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                extracted_data.append({
                    "video_id": item['id'],
                    "title": item['snippet'].get('title'),
                    "publishedAt": item['snippet'].get('publishedAt'),
                    "duration": item['contentDetails'].get('duration'),
                    "viewCount": item['statistics'].get('viewCount'),
                    "likeCount": item['statistics'].get('likeCount'),
                    "commentCount": item['statistics'].get('commentCount')
                })
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"/opt/airflow/data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)