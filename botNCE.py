import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import psycopg2
from decouple import config

# Creating list to append tweet data to
tweets_list2 = []

#List of users to search
users_list2 = [
'Airbus',
'Allianz',
'apoBank',
'BASF',
'BMW',
'commerzbank',
'Daimler',
'eonenergyuk',
'ericsson',
'jimmychoo',
'MLP_SE',
'Nestle',
'Philips',
'Saab',
'SAP',
'SKFgroup',
'skmdk',
'Syngenta',
'TelenorGroup',
'thyssenkrupp_en',
'uniper_energy',
'VattenfallGroup',
'VodafoneGroup',
'VW'
]
#List of hashtags to search
hashtags_list = [
'Innovation',
'Innovative',
'Innovators'
]

# Join the OR string to each list item
orString = ' OR '
orFromString = ' OR from:'
hashtags_list_or = orString.join(hashtags_list)
users_list_or2 = orFromString.join(users_list2)

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:' + users_list_or2 + ' ' + hashtags_list_or + ' since:2021-01-01 until:2021-12-31').get_items()):
    if i>1000:
        break
    tweets_list2.append([tweet.id, tweet.date, tweet.user.username, tweet.content.replace('\n','')])

print('from:' + users_list_or2 + ' ' + hashtags_list_or + ' since:2021-01-01 until:2021-12-31')

#Taking the list of hashtags from the tweet strings
for i, x in enumerate(tweets_list2):
    hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[3])
    print(x[1], hashtags)
  
# # Creating a dataframe from the tweets list above
tweets_df = pd.DataFrame(tweets_list2, columns=['TweetId', 'Datetime', 'TwitterHandle', 'Tweetcontent'])

# # Export dataframe into a CSV
tweets_df.to_csv('tweetsnce.csv', sep='`', index=False)

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create the table in SQL
cur.execute("""
    CREATE TABLE tweetsnce(
    TweetId bigint PRIMARY KEY,
    Datetime text,
    TwitterHandle text,
    Tweetcontent text
)
""")

#Copy data from csv into the SQL table
with open('tweetsnce.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'tweetsnce', sep='`')

#Commit the transaction
conn.commit()