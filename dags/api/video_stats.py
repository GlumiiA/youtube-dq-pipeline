import requests, json
import os

from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env") -- not needed anymore since we are using Airflow Variables to store the API key and channel handle

from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlist_id():

    try:

        url=f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle=@{CHANNEL_HANDLE}&key={API_KEY}'

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data, indent=4))

        data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        channel_items= data["items"][0]

        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        print(channel_playlistId)

        return channel_playlistId
    
    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlist_id):

    video_ids = []

    pageToken = None

    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}'

    try:

        while True:

            url = base_url

            if pageToken:
                url += f'&pageToken={pageToken}'

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            # Iterate over each playlist item to extract video IDs
            for item in data.get("items", []): 
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)


            pageToken = data.get("nextPageToken")

            if not pageToken:
                break
            
        return video_ids

    except requests.exceptions.RequestException as e:    
        raise e    
    
@task
def extract_video_data(video_ids):

    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        # video_id_lst is the full list of video IDs to process.
        # The function yields sublists of length batch_size for each API request.
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id:video_id + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            # batch is a subset of video_ids up to maxResults long.
            # Join the IDs into a comma-separated string for the videos API call.
            video_ids_str = ",".join(batch)
            
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}'

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                content_details = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "published_at": snippet["publishedAt"],
                    "duration": content_details["duration"],
                    "view_count": statistics.get("viewCount", None),
                    "like_count": statistics.get("likeCount", None),
                    "comment_count": statistics.get("commentCount", None) 
                } 

                extracted_data.append(video_data)

        return extracted_data




    except requests.exceptions.RequestException as e:
        raise e


@task
def save_to_json(extracted_data):
    os.makedirs("./data", exist_ok=True)
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
