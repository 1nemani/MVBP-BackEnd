from sweetagram_instagram_scrape_1 import INPUT_RUN_ID
from sweetagram_instagram_scrape_1 import INPUT_FILE
from sweetagram_instagram_scrape_1 import INPUT_SHEET
from sweetagram_instagram_scrape_1 import INPUT_DATABASE_NAME
from sweetagram_instagram_scrape_1 import INPUT_PROXY_USERNAME
from sweetagram_instagram_scrape_1 import INPUT_PROXY_PASSWORD
from sweetagram_instagram_scrape_1 import INPUT_PROXY_PORT 
from sweetagram_instagram_scrape_1 import INPUT_USE_PROXIES
from sweetagram_instagram_scrape_1 import INPUT_BATCH_SIZE
from sweetagram_instagram_scrape_1 import Sweetagram_And_Instagram_Scraper


# install: requests, lxml, openpyxl
import requests
from lxml import html
from openpyxl import load_workbook
import os
import sys
import sqlite3
import bz2
import json
import pickle
import time
from datetime import datetime
import threading
import pprint
import random
import re
import csv



class Sweetagram_And_Instagram_Writer(Sweetagram_And_Instagram_Scraper):
    def write_data(self):
        if self.inputs_are_good == False or self.is_interrupted == True:
            return

        print("Writing data...")
        # create output file
        HEADERS = ['Username', 'Found under tag', 'Followers', 'Following', 'Total posts', 'Total recent posts', 'Total recent likes', 'Total recent comments', 'Recent avg likes per post', 'Recent avg comments per post',
                   'Date of most recent post', 'Category', 'Biography text', 'Is verified', 'Business email', 'Business phone', 'Time of scraping']
        outfile_name = datetime.now().strftime("%d-%B-%Y %H_%M_%S") + " instagram_users_data.csv"
        outfile = open(outfile_name, 'w', newline='', encoding='utf-8')
        writer = csv.writer(outfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        ccc = writer.writerow(HEADERS)
        
        
        # fetch everything and sort, parse!
        fetcher = self.db_cursor.execute("SELECT * FROM InstagramUsersTable WHERE json_data IS NOT NULL ORDER BY timestamp DESC") # remove limit
        total_written = 0
        for fetched_row in fetcher:
            parsed_data = self.parse_data(fetched_row[0], fetched_row[1], fetched_row[2], fetched_row[3])
            row_to_write = []
            for header_item in HEADERS:
                row_to_write.append(parsed_data[header_item])
            ccc = writer.writerow(row_to_write)

            total_written+=1
            if total_written%1000 == 0:
                print("Total written so far:", total_written)

        outfile.close()
        print("Created output file:", outfile_name)
        return


    def parse_data(self, input_username, input_found_under_tag, input_json, input_scrapetime):
        data_to_return = {}
        data_to_return["Username"] = input_username
        data_to_return["Found under tag"] = input_found_under_tag
        data_to_return["Time of scraping"] = input_scrapetime
        data_to_return["Followers"] = ''
        data_to_return["Following"] = ''
        
        data_to_return["Total recent posts"] = 0
        data_to_return["Total recent likes"] = '' # can't get for all posts
        data_to_return["Total recent comments"] = '' # can't get for all posts
        data_to_return["Recent avg likes per post"] = ''
        data_to_return["Recent avg comments per post"] = ''
        data_to_return["Date of most recent post"] = ''
        most_recent_post_time = -1
        recent_post_items = [] # calculate totals and averages from this!
        
        data_to_return["Category"] = ''
        data_to_return["Biography text"] = ''
        data_to_return["Is verified"] = ''
        data_to_return["Business email"] = ''
        data_to_return["Business phone"] = ''
        
        # parse out data from json
        loaded_json = pickle.loads(bz2.decompress(input_json))
        try:
            data_to_return["Followers"] = loaded_json['graphql']['user']['edge_followed_by']["count"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Following"] = loaded_json['graphql']['user']['edge_follow']["count"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Total posts"] = loaded_json['graphql']['user']['edge_owner_to_timeline_media']["count"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Category"] = loaded_json['graphql']['user']["category_name"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Biography text"] = loaded_json['graphql']['user']["biography"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Is verified"] = loaded_json['graphql']['user']["is_verified"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Business email"] = loaded_json['graphql']['user']["business_email"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        try:
            data_to_return["Business phone"] = loaded_json['graphql']['user']["business_phone_number"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass


        # get recent likes and comment counts
        post_list_to_iterate = []
        try:
            post_list_to_iterate = loaded_json['graphql']['user']['edge_owner_to_timeline_media']["edges"]
        except (KeyError, TypeError, IndexError, ValueError):
            pass
        if type(post_list_to_iterate) == list:
            for one_post_item in post_list_to_iterate:
                if type(one_post_item) == dict:
                    post_to_add = {"like_count":None, "comment_count":None}
                    try:
                        post_to_add["like_count"] = one_post_item["node"]["edge_liked_by"]["count"]
                        post_to_add["comment_count"] = one_post_item["node"]["edge_media_to_comment"]["count"]
                    except (KeyError, TypeError, IndexError, ValueError):
                        pass

                    # determine post time
                    try:
                        post_stamp = one_post_item["node"]["taken_at_timestamp"]
                        if type(post_stamp) in [int, float]:
                            post_stamp = int(post_stamp)
                            if post_stamp > most_recent_post_time:
                                most_recent_post_time = post_stamp
                    except (KeyError, TypeError, IndexError, ValueError):
                        pass

                    # add if good
                    if type(post_to_add["like_count"]) == int and type(post_to_add["comment_count"]) == int:
                        recent_post_items.append(post_to_add)

        if len(recent_post_items) != 0:
            # calculate averages and totals
            data_to_return["Total recent posts"] = len(recent_post_items)
            total_recent_likes = sum([post_item["like_count"] for post_item in recent_post_items])
            total_recent_comments = sum([post_item["comment_count"] for post_item in recent_post_items])
            data_to_return["Total recent likes"] = total_recent_likes
            data_to_return["Total recent comments"] = total_recent_comments
            
            data_to_return["Recent avg likes per post"] = round(total_recent_likes/len(recent_post_items), 2)
            data_to_return["Recent avg comments per post"] = round(total_recent_comments/len(recent_post_items), 2)

        if most_recent_post_time != -1:
            try:
                data_to_return["Date of most recent post"] = datetime.utcfromtimestamp(most_recent_post_time).strftime("%d-%B-%Y %H:%M:%S")
            except OSError:
                pass


        for key_to_check in data_to_return:
            if data_to_return[key_to_check] == None:
                data_to_return[key_to_check] = ''
                
        return data_to_return


if __name__ == '__main__':
    writ = Sweetagram_And_Instagram_Writer(INPUT_RUN_ID, INPUT_FILE, INPUT_SHEET, INPUT_DATABASE_NAME, INPUT_PROXY_USERNAME, INPUT_PROXY_PASSWORD, INPUT_PROXY_PORT, INPUT_USE_PROXIES, INPUT_BATCH_SIZE)
    writ.write_data()
