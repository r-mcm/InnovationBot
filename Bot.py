import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import psycopg2
from decouple import config

# Creating list to append tweet data to
tweets_list = []

#List of users to search
users_list = [
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
hashtags_list_or = orString.join(hashtags_list)
users_list_or = orString.join(users_list)

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:' + users_list_or + '' + hashtags_list_or + ' since:2021-01-01 until:2021-12-31').get_items()):
    if i>10:
        break
    tweets_list.append([tweet.id, tweet.date, tweet.user.username, tweet.content.replace('\n','')])

#Taking the list of hashtags from the tweet strings
for i, x in enumerate(tweets_list):
    hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[2])
    print(x[1], hashtags)
  
# # Creating a dataframe from the tweets list above
tweets_df = pd.DataFrame(tweets_list, columns=['TweetId', 'Datetime', 'Username', 'Text'])

# # Export dataframe into a CSV
tweets_df.to_csv('tweets.csv', sep='`', index=False)

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create the table in SQL
cur.execute("""
    CREATE TABLE tweets(
    TweetId bigint PRIMARY KEY,
    Datetime text,
    Username text,
    Tweetcontent text
)
""")

#Copy data from csv into the SQL table
with open('tweets.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'tweets', sep='`')

#Commit the transaction
conn.commit()