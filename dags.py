from airflow.decorators import dag, task
import pendulum
import xmltodict
import requests
import os
import urllib.request
from datetime import timedelta
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

###### Use <airflow dags list-import-error> in console to debug the dags error.

url = "https://www.marketplace.org/feed/podcast/marketplace/"
default_args = {
    'owner': 'sanjog',
    'retries': 5,
    'retry_delay' : timedelta(minutes = 2)
}

#defining dags
@dag(
    dag_id = 'podcast_summary08',
    default_args = default_args,
    start_date = pendulum.datetime(2023,6,1),
    schedule_interval = '0 0 * * *',    ##scheduling daily by using cron expression
    catchup = False
)

def podcast_summary():

    create_database = SqliteOperator(
        task_id = 'create_table_sqlite',
        sql = r"""
        CREATE TABLE IF NOT EXISTS episodes(
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT
        );
        """,
        sqlite_conn_id = 'podcasts'
    )

    @task
    def get_episode(podcast_url):
        data = requests.get(podcast_url)
        feed = xmltodict.parse(data.text)
        episodes = feed['rss']['channel']['item']   ##Description of the episode is under the given fields.
        print(f"Found {len(episodes)} episodes")
        return episodes

    podcast_episodes = get_episode(url)
    create_database.set_downstream(podcast_episodes)
    
    @task
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id = 'podcasts')
        stored =  hook.get_pandas_df("Select * from  episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored["link"].values:    ##used for storing only new podcasts
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append((episode["link"], episode["title"], episode["pubDate"], episode["description"],filename))
    
        columns = ["link", "title","published","description", "filename"]
        hook.insert_rows(table = 'episodes', rows = new_episodes, target_fields = columns)
        print("Rows inserted successfully")

    load_episodes(podcast_episodes)


    @task
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_url = episode["enclosure"]["@url"]
            audio_path = f"podcast_summary/podcast_episodes/{filename}"
            print(os.listdir())
            if filename in os.listdir("podcast_summary/podcast_episodes"):
                pass
            else:
                print(f"Downloading {filename}..............")
                urllib.request.urlretrieve(audio_url,audio_path)

        
    download_episodes(podcast_episodes)

summary = podcast_summary()