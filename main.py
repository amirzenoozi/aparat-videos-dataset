__author__ = "Amirhossein Douzendeh Zenoozi"
__license__ = "MIT"
__version__ = "1.0"


from openpyxl import load_workbook
from tqdm import tqdm

import pandas as pd

import random
import time
import requests
import sqlite3

class AparatCrawler:
    def __init__(self, **kwargs):
        # DataBase Connection Config
        self.db = sqlite3.connect('aparat.db')
    
        try:
            self.create_video_table()
        except sqlite3.Error as error:
            print('========== VideoTable Error ==========')
            print(error)
            print('========== End of VideoTable Error ==========')
            pass
    
        try:
            self.create_categories_table()
        except sqlite3.Error as error:
            print('========== CategoryTable Error ==========')
            print(error)
            print('========== End of CategoryTable Error ==========')
            pass

    def create_video_table(self):
        self.db.execute('''CREATE TABLE videos
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id CHAR(50),
                title TEXT NOT NULL,
                description TEXT,
                username CHAR(50),
                duration INTEGER,
                date DATETIME,
                cat_id INTEGER,
                like_count INTEGER,
                visit_count INTEGER);''')

    def create_categories_table(self):
        self.db.execute('''CREATE TABLE categories
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                cat_id CHAR(50),
                name CHAR(50),
                link CHAR(50),
                video_count INTEGER);''')

    def is_video_processed(self, video_id):
        database_record = self.db.execute("""SELECT video_id FROM videos WHERE video_id = (?) LIMIT 1""", (video_id,)).fetchone()
        return bool(database_record)

    def insert_video_item_to_db(self, video_id, username, description, duration, like_count, visit_count, title, date, cat_id):
        try:
            self.db.execute("""INSERT INTO videos (video_id, username, description, duration, like_count, visit_count, title, date, cat_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (video_id, username, description, duration, like_count, visit_count, title, date, cat_id))
            self.db.commit()
        except sqlite3.Error as error:
            print(error)
            pass

    def insert_cat_item_to_db(self, cat_id, name, link, video_count):
        try:
            self.db.execute("""INSERT INTO categories (cat_id, name, link, video_count) VALUES (?, ?, ?, ?)""", (cat_id, name, link, video_count))
            self.db.commit()
        except sqlite3.Error as error:
            print(error)
            pass

    def update_cat_item(self, cat_id, name, link, video_count):
        try:
            self.db.execute("""UPDATE categories SET name = (?), link = (?), video_count = (?) WHERE cat_id = (?)""", (name, link, video_count, cat_id))
            self.db.commit()
        except sqlite3.Error as error:
            print(error)
            pass

    def is_cat_item_exist(self, cat_id):
        database_record = self.db.execute("""SELECT cat_id FROM categories WHERE cat_id = (?) LIMIT 1""", (cat_id,)).fetchone()
        return bool(database_record)

    def get_data(self, url):
        try:
            req = requests.get(url)
            data = req.json()
            return data
        except:
            print('================ Error ================')
            print(f'Error in {req.status_code}')
            return None

    def close_db( self ):
        self.db.close()

    def get_home_page(self, page_count):
        for i in tqdm(range(page_count)):
            if i == 0:
                url = 'https://www.aparat.com/api/fa/v1/video/video/list/tagid/1'
            else:
                url = data['links']['next']
        
            data = self.get_data(url)
            if data is not None and data != []:
                for index, record in enumerate(data['included']):
                    if record['type'] == 'Video' and not self.is_video_processed(record['attributes']['id']):
                        video_id = record['attributes']['id']
                        username = record['attributes']['username']
                        description = record['attributes']['description']
                        duration = record['attributes']['duration']
                        like_count = record['attributes']['like_cnt']
                        visit_count = record['attributes']['visit_cnt_int']
                        title = record['attributes']['title']
                        date = record['attributes']['sdate_rss']
                        cat_id = record['attributes']['catId']
                        # Insert Data to DB
                        self.insert_video_item_to_db(video_id, username, description, duration, like_count, visit_count, title, date, cat_id)
            # Set Random Sleep
            time.sleep(random.randint(1, 5))

    def get_all_categories(self):
        data = self.get_data('https://www.aparat.com/etc/api/categories')

        if data is not None and data != []:
            for index, record in enumerate(data['categories']):
                    cat_id = record['id']
                    name = record['name']
                    link = record['link']
                    count = record['videoCnt']
                    if not self.is_cat_item_exist(cat_id):
                        self.insert_cat_item_to_db(cat_id, name, link, count)
                    else:
                        self.update_cat_item(cat_id, name, link, count)
                    
    def get_single_categories(self, cat_number, page=10):
        is_first_call = True
        processed_page = 0
        if is_first_call:
            url = f'https://www.aparat.com/etc/api/categoryvideos/perpage/30/cat/{cat_number}'
        else:
            url = data['ui']['pagingForward'].replace('//etc/', '/etc/')
    
        while processed_page < page:
            data = self.get_data(url)
            if data is not None and data != []:
                for index, record in enumerate(data['categoryvideos']):
                    if not self.is_video_processed(record['id']):
                        video_id = record['id']
                        username = record['username']
                        description = 'None'
                        duration = record['duration']
                        like_count = -1
                        visit_count = record['visit_cnt']
                        title = record['title']
                        date = record['create_date']
                        cat_id = cat_number
                        # Insert Data to DB
                        self.insert_video_item_to_db(video_id, username, description, duration, like_count, visit_count, title, date, cat_id)
            # Set Random Sleep
            time.sleep(random.randint(1, 5))

def main():
    aparat_crawler = AparatCrawler()
    aparat_crawler.get_home_page(100)
    # aparat_crawler.get_all_categories()
    # aparat_crawler.get_single_categories(22, 10)
    aparat_crawler.close_db()

if __name__ == '__main__':
    main()