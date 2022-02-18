import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import psycopg2
from decouple import config
import csv

bigkeyword_list = []

# reading the csv into a list
file = open("bigtweetsunion.csv", encoding="utf8")
csv_reader = csv.reader(file)

big_tweets_list_combined = []
for row in csv_reader:
    big_tweets_list_combined.append(row)

# # Creating a dataframe from the tweets list above
bigtweets_df = pd.DataFrame(big_tweets_list_combined, columns=['TweetId', 'Datetime', 'TwitterHandle', 'Tweetcontent'])

#Taking the list of hashtags from the tweet strings
keyword_id = 0
for i, x in enumerate(big_tweets_list_combined):
    if re.findall(r"#[a-zA-Z0-9_]*", x[3]):
        hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[3])
        for i, h in enumerate(hashtags):
            keyword_id += 1
            bigkeyword_list.append([keyword_id, x[0], h])
  
# Creating a dataframe hashtags above
bigkeyword_df = pd.DataFrame(bigkeyword_list, columns=['KeywordID', 'TweetID', 'Keyword'])

# Export dataframes into CSV's
bigkeyword_df.to_csv('bigkeywords.csv', sep=',', index=False)

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create the tweets table in SQL
cur.execute("""
    CREATE TABLE bigkeywords(
    KeywordID int PRIMARY KEY,
    TweetId bigint,
    Keyword text
)
""")

#Copy data from csv into the SQL table for tweets
with open('bigkeywords.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'bigkeywords', sep=',')

#Commit the transaction
conn.commit()