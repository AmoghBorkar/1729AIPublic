# This script loads the JSON dump from twitter stream, reads the main content and writes it to a csv file
import os
import sys
import time
import json
from datetime import datetime, date
import unicodecsv as csv
from bs4 import BeautifulSoup
import pandas as pd

start_time = time.time()
print('Start')

INPUT_DIR = 'C:\\Users\\amogh\\Desktop\\Work\\ProjectsGIT\\1729AIDesktop\\Tweets'
OUPTUT_DIR = 'C:\\Users\\amogh\\Desktop\\Work\\ProjectsGIT\\1729AIDesktop\\TweetsCSV'

while True:
    json_files = [file_json for file_json in os.listdir(INPUT_DIR) if file_json.endswith('.json')]
    for i in range(0, len(json_files),1):
        os.chdir(INPUT_DIR)
        with open(json_files[i],'r') as infile:
            temp = json.load(infile)

        try:
            temp_parsed = {}
            if 'extended_tweet' in temp.keys():
                temp_parsed['text'] = temp['extended_tweet']['full_text']
            else:
                temp_parsed['text'] = temp['text']
            # reference - https://stackoverflow.com/questions/7703865/going-from-twitter-date-to-python-datetime-date
            tweet_datetime = datetime.strptime(temp['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
            temp_parsed['tweet_date'] = str(tweet_datetime.date().isoformat())
            temp_parsed['tweet_time_gmt'] = str(tweet_datetime.time().isoformat())
            # Parse and get the device from URL
            soup = BeautifulSoup(temp['source'], "html.parser")
            temp_parsed['tweet_source'] = temp['source']
            temp_parsed['tweet_source_clean'] = soup.find('a').contents[0]

            if 'place' in temp.keys():
                temp_parsed['place_type'] = temp['place']['place_type']
                temp_parsed['place_name'] = temp['place']['name']
                temp_parsed['place_full_name'] = temp['place']['full_name']
                temp_parsed['place_country_code'] = temp['place']['country_code']
                temp_parsed['place_country'] = temp['place']['country']
                if 'bounding_box' in temp['place']:
                    if temp['place']['bounding_box']['type'] == 'Point':
                        temp_parsed['place_lat'] = str(temp['place']['bounding_box']['coordinates'][0][1])
                        temp_parsed['place_long'] = str(temp['place']['bounding_box']['coordinates'][0][0])
                        temp_parsed['place_lat1'] = '0'
                        temp_parsed['place_lat2'] = '0'
                        temp_parsed['place_lat3'] = '0'
                        temp_parsed['place_lat4'] = '0'
                        temp_parsed['place_long1'] = '0'
                        temp_parsed['place_long2'] = '0'
                        temp_parsed['place_long3'] = '0'
                        temp_parsed['place_long4'] = '0'
                        
                    elif temp['place']['bounding_box']['type'] == 'Polygon':
                        place_long1 = temp['place']['bounding_box']['coordinates'][0][0][0]
                        place_lat1 = temp['place']['bounding_box']['coordinates'][0][0][1]

                        place_long2 = temp['place']['bounding_box']['coordinates'][0][1][0]
                        place_lat2 = temp['place']['bounding_box']['coordinates'][0][1][1]

                        place_long3 = temp['place']['bounding_box']['coordinates'][0][2][0]
                        place_lat3 = temp['place']['bounding_box']['coordinates'][0][2][1]

                        place_long4 = temp['place']['bounding_box']['coordinates'][0][3][0]            
                        place_lat4 = temp['place']['bounding_box']['coordinates'][0][3][1]

                        place_long = (place_long1 + place_long2 + place_long3 + place_long4)/4
                        place_lat = (place_lat1 + place_lat2 + place_lat3 + place_lat4)/4

                        temp_parsed['place_long'] = str(place_long)
                        temp_parsed['place_lat'] = str(place_lat)

                        temp_parsed['place_lat1'] = str(place_lat1)
                        temp_parsed['place_lat2'] = str(place_lat2)
                        temp_parsed['place_lat3'] = str(place_lat3)
                        temp_parsed['place_lat4'] = str(place_lat4)
                        temp_parsed['place_long1'] = str(place_long1)
                        temp_parsed['place_long2'] = str(place_long2)
                        temp_parsed['place_long3'] = str(place_long3)
                        temp_parsed['place_long4'] = str(place_long4)

                    else:
                        temp_parsed['place_lat1'] = '0'
                        temp_parsed['place_lat2'] = '0'
                        temp_parsed['place_lat3'] = '0'
                        temp_parsed['place_lat4'] = '0'
                        temp_parsed['place_long1'] = '0'
                        temp_parsed['place_long2'] = '0'
                        temp_parsed['place_long3'] = '0'
                        temp_parsed['place_long4'] = '0'
                        temp_parsed['place_long'] = '0'
                        temp_parsed['place_lat'] = '0'

            temp_parsed['language'] = temp['lang']
            if 'possibly_sensitive' in temp.keys():
                temp_parsed['possibly_sensitive'] = str(temp['possibly_sensitive'])
            else:
                temp_parsed['possibly_sensitive'] = 'NA'
        
            os.remove(json_files[i])

        except KeyboardInterrupt:
            raise
        except Exception as ex:
            template = 'An exception of type (0) occured. Arguments:\n{1!r}'
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            time.sleep(0.1)
            i+=1
            continue

        os.chdir(OUPTUT_DIR)
        now = date.today()
        filename = 'tweetdata'+now.strftime("%Y%m%d")+".csv"
        if os.path.exists(filename):
            append_write = 'ab'
        else:
            append_write = 'wb'
        with open('tweetdata'+now.strftime("%Y%m%d")+".csv", append_write) as filedata:
            try:
                w = csv.DictWriter(filedata, delimiter = ',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n', fieldnames=temp_parsed.keys())
                if append_write == 'wb':
                    w.writeheader()
                w.writerow(temp_parsed)

            except KeyboardInterrupt:
                raise
            except Exception as ex:
                template = 'An exception of type (0) occured. Arguments:\n{1!r}'
                message = template.format(type(ex).__name__, ex.args)
                print(message)
                time.sleep(0.1)
                i+=1
                continue
            