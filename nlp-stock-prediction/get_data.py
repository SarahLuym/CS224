import pandas as pd
import numpy as np
import datetime as dt
import time
import os
import requests


def download_reddit_data(filename, object_type, subreddit, start_date, end_date):
    start_date = convert_date_to_timestamp(start_date)
    end_date = convert_date_to_timestamp(end_date)
    url = "https://api.pushshift.io/reddit/{}/search?limit=1000&sort=desc&subreddit={}&before={}&after={}"
    print(f"Saving {object_type}s to {filename}")
    dfs = []
    previous_end_date = end_date
    while True:
        new_url = url.format(object_type, subreddit, previous_end_date, start_date)
        time.sleep(1)  # pushshift有一个速率限制，如果我们发送请求太快，它将开始返回错误消息
        try:
            json_text = requests.get(new_url, headers={'User-Agent': "Post downloader by /u/Watchful1"})
            json_data = json_text.json()
        except:
            time.sleep(1)
            continue
        if 'data' not in json_data:
            break
        objects = json_data['data']

        if len(objects) == 0:
            break
        df = pd.DataFrame(objects)
        previous_end_date = min(df['created_utc'])
        dfs.append(df)

    all_reddit_df = pd.concat(dfs).reset_index()
    all_reddit_df.to_csv(filename)


def convert_date_to_timestamp(datestring: str):
    date_timestamp = dt.datetime.timestamp(dt.datetime.strptime(datestring, "%d/%m/%Y"))
    return int(date_timestamp)


# start_date = '21/5/2021'
# end_date = '21/5/2022'
# subreddit = 'wallstreetbets'
#
# download_reddit_data('reddit_wallstreetbets.csv', "submission", subreddit, start_date, end_date)


df = pd.read_csv('reddit_wallstreetbets.csv')
df = df[['created_utc', 'title', 'selftext', 'upvote_ratio', 'score']]
df['date'] = df['created_utc'].apply(lambda x: dt.datetime.fromtimestamp(x))


