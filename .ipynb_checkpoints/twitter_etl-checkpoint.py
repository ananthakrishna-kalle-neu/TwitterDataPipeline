import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs
import os

def run_twitter_etl():
    access_key = "dmpNtePFjcQ03lPQy2GoGmlIH" 
    access_secret = "nCML8AVqpZtiQlmjJSFeXZh8BaDvSFfTz5WZ2dg9gbFX5P4AjP" 
    consumer_key = "3252407862-cAchqMILTCeZUtrw7aHBzlrYYo1CxZpAZzwF9yU"
    consumer_secret = "ap9MBiERnYlNKOTnu7h4hPU6O3OlN7P0Xzs2YBCStkfFl"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 
    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )
    print(tweets)
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('refined_tweets.csv')
