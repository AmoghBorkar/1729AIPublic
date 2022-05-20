# This script connects to twitter for listening to global tweets and saves a large number into a json

import sys
import os
import tweepy
import numpy as np 
import time
import json
from datetime import date, timedelta
import csv

# Define the keys
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

#auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#auth.set_access_token(access_key, access_secret)

#api = tweepy.API(auth, wait_on_rate_limit=True, retry_count=10, retry_delay=5,retry_errors=5)


GEOBOX_UK = [-7.57216793459, 49.959999905, 1.68153079591, 58.6350001085]
#GEBOX_Indonesia = [05.82,-6.68,119.86,-4.76]
INTERMEDIATE_FILES_LOCATION = "C:\\Users\\amogh\\Desktop\\Work\\ProjectsGIT\\1729AIDesktop\\Tweets"
FINAL_FILES_LOCATION = ""


CHUNK_SIZE = 1000
temp_status = {}

class  CustomStreamListener(tweepy.Stream):
    def on_status(self, status):
        if len(status.text.encode('utf-8')) > 15 and status.lang is not None and status.place is not None:
            # temp = {}
            # temp['tweet_text'] = status.text
            # print(status)
            # print(status.coordinates)
            # temp['tweet_id'] = status.id_str
            # temp['temp_time_gmt'] = status.created_at.time().isoformat()
            # temp['tweet_language'] = status.lang

            with open('tweet_data_'+str(status.id)+'.json','w') as outfile:
                json_str = json.dumps(status._json)
                outfile.write(json_str)


def on_error(self,status_code):
    print (sys.stderr, 'Encountered error with status code:', status_code)
    print ('Sleeping for 20 seconds before proceeding.')
    time.sleep(20)
    return True

def on_timeout(self):
    print (sys.stderr, 'Timeout...')
    print ('Sleeping for 2 minutes before proceeding.')
    time.sleep(120)
    return True 

def start_stream():
    os.chdir(INTERMEDIATE_FILES_LOCATION)
    while True:
        try:
            sapi = CustomStreamListener(consumer_key, consumer_secret, access_token, access_token_secret)
            sapi.filter(locations=GEOBOX_UK)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            template = 'An exception of type {0} occured. Arguments: \n{1!r}'
            message = template.format(type(ex).__name__, ex.args)
            print (message)
            print ('re-setting the lists.')
            print ('Other exception - Sleeping for 10 seconds before proceeding.')
            time.sleep(10)
            continue


if __name__ == '__main__':
    start_stream()       