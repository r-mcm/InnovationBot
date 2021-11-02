import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import psycopg2
from decouple import config

# Creating list to append tweet data to
tweets_list1 = []

#List of users to search
users_list1 = [
'Aon_plc',
'AWE_plc',
'BAESystemsplc',
'bp_plc',
'DefenceHQ',
'DefraGovUK',
'DeutscheBank',
'DHSCgovuk',
'DWP',
'FedEx',
'Leonardo_live',
'LloydsBank',
'LondonLmg',
'metpoliceuk',
'MicroFocus',
'MOFUAE',
'MoJGovUK',
'NatWestGroup',
'networkrail',
'RollsRoyce',
'ukhomeoffice'
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
users_list_or1 = orFromString.join(users_list1)

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:' + users_list_or1 + ' ' + hashtags_list_or + ' since:2021-01-01 until:2021-12-31').get_items()):
    if i>1000:
        break
    tweets_list1.append([tweet.id, tweet.date, tweet.user.username, tweet.content.replace('\n','')])

#Taking the list of hashtags from the tweet strings
for i, x in enumerate(tweets_list1):
    hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[3])
    print(x[1], hashtags)
  
# # Creating a dataframe from the tweets list above
tweets_df1 = pd.DataFrame(tweets_list1, columns=['TweetId', 'Datetime', 'TwitterHandle', 'Tweetcontent'])

# # Export dataframe into a CSV
tweets_df1.to_csv('tweetsuk.csv', sep='`', index=False)

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create the table in SQL
cur.execute("""
    CREATE TABLE tweetsuk(
    TweetId bigint PRIMARY KEY,
    Datetime text,
    TwitterHandle text,
    Tweetcontent text
)
""")

#Copy data from csv into the SQL table
with open('tweetsuk.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'tweetsuk', sep='`')

#Commit the transaction
conn.commit()