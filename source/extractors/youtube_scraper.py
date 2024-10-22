import pandas as pd
from pytube import YouTube
import re

def scrape_youtube_data(input_path):
    
    try:
        df = pd.read_csv(input_path)
        
        def get_video_data(video_url):
            try:
                video_id = re.search(r'(?<=v=)[^&#]+', video_url).group()
                yt = YouTube(f'https://www.youtube.com/watch?v={video_id}')
                return {
                    'publish_date': yt.publish_date.strftime('%Y-%m-%d'),
                    'current_likes_count': yt.initial_data['videoDetails']['likes']
                }
            except Exception as e:
                print(f"Error processing video {video_url}: {str(e)}")
                return {'publish_date': None, 'current_likes_count': None}

        # Add YouTube data
        youtube_data = df['Video_Url'].apply(get_video_data)
        df['publish_date'] = youtube_data.apply(lambda x: x['publish_date'])
        df['current_likes_count'] = youtube_data.apply(lambda x: x['current_likes_count'])
        

        return df
        
    except Exception as e:
        raise Exception(f"Error scraping YouTube data: {str(e)}")