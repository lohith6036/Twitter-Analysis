import json
import tweepy
from textblob import TextBlob

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        if status.retweeted:
            return
        # print(status._json)
        tweet = status._json
        blob = TextBlob(status.text)
        sent = blob.sentiment
        b = {"pol": str(sent.polarity), "subjectivity": str(sent.subjectivity)}
        tweet.update(b)
        # print(json.dumps(tweet))
        # f.write(json.dumps(tweet) + "\n")


    def on_error(self, status_code):
        if status_code == 420:
            return False
twitter_key = 'gSEKhW3jUNeWSE7UDcx2FaGS5'
twitter_secret_key = 'bPlz8kEBJWsQqg6sVLuZaYcJO0b9WBBOFpxCJASwxk2TMdVO2E'
access_key = '1066068152-3qMacgRh4mYtA5WBA44ldHSyqO0qp25Vub4w9Ie'
access__secret_key = 'yYp4RGJDiQqh0XF2D6gBOopG3zaNcPlDpfmFAOZay3VMA'
f = open("project_tweets_file.json","a+")
authorization = tweepy.OAuthHandler(twitter_key, twitter_secret_key)
authorization.set_access_token(access_key, access__secret_key)
api = tweepy.API(authorization)
streamer = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=streamer)
stream.filter(track=["Democratic Party","Republican Party","Democrats","Republican","GOP","democratic","Republics"])