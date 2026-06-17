import logging
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



class YoutubeIngester:

    def __init__(self, api_key, keyword, published_after, published_before,
             region_code, video_type, max_results):
        
        self.api_key = api_key
        self.keyword = keyword
        self.published_after = published_after
        self.published_before = published_before
        self.region_code = region_code
        self.type = video_type
        self.max_results = max_results
        self.youtube = self._build_service()

    def _build_service(self):

        return build("youtube", "v3", developerKey=self.api_key)

    def _get_videos(self):

        request = self.youtube.search().list(
        q=self.keyword,
        part = "snippet",
        publishedAfter=self.published_after,
        type=self.type,
        regionCode=self.region_code,
        maxResults=self.max_results)
        
        response = request.execute()      # MISSING — actually execute the call
        return response 

    def _get_comments(self,video_id):

        request = self.youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100,
        order="time"
    )
        response = request.execute()
        return response



    def ingest(self):

        video_response = self._get_videos()

        videos = []
        all_comments = []

        for item in video_response["items"]:
            video = {
                "video_id": item["id"]["videoId"],
                "title": item["snippet"]["title"],
                "channel_name": item["snippet"]["channelTitle"],
                "publishedAt": item["snippet"]["publishedAt"],
            }

            videos.append(video)

        for video in videos:
            comment_response = self._get_comments(video["video_id"])

            for item in comment_response["items"]:
                comment = {
                    "textDisplay" : item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "authorDisplayName" : item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "likeCount" : item["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                    "publishedAt" : item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                    "totalReplyCount" : item["snippet"]["totalReplyCount"],
                    "comment_id" : item["snippet"]["topLevelComment"]["id"],

                }

                all_comments.append(comment)

                

        import pandas as pd
        videos_df = pd.DataFrame(videos)
        comments_df = pd.DataFrame(all_comments)
    
        return videos_df, comments_df
    
if __name__ == "__main__":
    load_dotenv("config/secrets_template.env")
    api_key = os.getenv("YOUTUBE_API_KEY")

    ingester = YoutubeIngester(
    api_key=api_key,
    keyword="Iran War Middle East 1979 ",
    published_after="2024-01-01T00:00:00Z",
    published_before="2026-01-01T00:00:00Z",
    region_code="IN",
    video_type="video",
    max_results=5
)

    videos_df, comments_df = ingester.ingest()
    print(videos_df)
    print(comments_df)
