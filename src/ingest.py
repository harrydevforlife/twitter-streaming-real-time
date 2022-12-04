import sys
sys.path.append(".")

from lib import apicredentials
import tweepy
from textblob import TextBlob
import time
import re
import json
import datetime
bearer_token = apicredentials.BEARER_TOKEN

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

# Fetch the service account key JSON file contents
cred = credentials.Certificate('./src/keys/twitter-streaming-adminsdk.json') 
# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    'databaseURL': "https://twitter-streaming-fc403-default-rtdb.firebaseio.com/"
})
ref = db.reference('Twitter Streaming')

def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None
class MyStreamer(tweepy.StreamingClient):
    new_tweet={}
    def on_tweet(self, tweet):
        self.new_tweet['id'] = tweet.id
        self.new_tweet['text'] = tweet.text
        a= tweet.created_at
        a= a.strftime("%Y-%m-%d %H:%M:%S")
        self.new_tweet['created_at'] =a#json.dumps(tweet.created_at, default=str)

       
        self.new_tweet['sentiment'] = TextBlob(self.new_tweet['text']).sentiment
        if self.new_tweet['sentiment'].polarity == 0 :
            self.new_tweet['polarity'] = self.new_tweet['sentiment'].polarity
        else :
            if self.new_tweet['sentiment'].polarity <0 :
                self.new_tweet['polarity']=-1
            else :
                self.new_tweet['polarity']=1


        self.new_tweet['subjectivity'] = self.new_tweet['sentiment'].subjectivity
        self.new_tweet['retweet_count'] = tweet.public_metrics['retweet_count']
        #  print(tweet.public_metrics)
        self.new_tweet['source'] = tweet.source
        # print(self.new_tweet)
        # Push data into Firebase Real-time Database

        print('===============================')
        time.sleep(4)
        
    def on_includes(self, includes):
        self.new_tweet['user_created_at']=json.dumps(includes["users"][0].created_at, default=str)
        self.new_tweet['user_location']=includes["users"][0].location
        self.new_tweet['user_description']=includes["users"][0].description
        self.new_tweet['user_followers_count']=includes["users"][0].public_metrics['followers_count']
        ref.push().set(self.new_tweet)


streamer = MyStreamer(bearer_token)
streamer.add_rules(tweepy.StreamRule('facebook -is:retweet '))
# query ='facebook -is:retweet -is:verified lang:en '
streamer.get_rules()
streamer.filter(tweet_fields = ["created_at", "text", "source","lang","geo","public_metrics"],
                expansions=['author_id',],
                user_fields = ["name", "username","created_at","location", "verified", "description","public_metrics"],
)

streamer.get_rules()

