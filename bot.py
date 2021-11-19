import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import psycopg2
from decouple import config

# Creating list to append tweet data to
tweets_list1 = []
tweets_list2 = []
keyword_list = []

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
users_list_or1 = orFromString.join(users_list1)
users_list_or2 = orFromString.join(users_list2)

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:' + users_list_or1 + ' ' + hashtags_list_or + ' since:2021-01-01 until:2021-12-31').get_items()):
    if i>10:
        break
    tweets_list1.append([tweet.id, tweet.date, tweet.user.username, tweet.content.replace('\n','')])

for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:' + users_list_or2 + ' ' + hashtags_list_or + ' since:2021-01-01 until:2021-12-31').get_items()):
    if i>10:
        break
    tweets_list2.append([tweet.id, tweet.date, tweet.user.username, tweet.content.replace('\n','')])

# # Creating a dataframe from the tweets list above
tweets_df1 = pd.DataFrame(tweets_list1, columns=['TweetId', 'Datetime', 'TwitterHandle', 'Tweetcontent'])
tweets_df2 = pd.DataFrame(tweets_list2, columns=['TweetId', 'Datetime', 'TwitterHandle', 'Tweetcontent'])

#Taking the list of hashtags from the tweet strings
tweets_list_combined = tweets_list1 + tweets_list2
keyword_id = 0
for i, x in enumerate(tweets_list_combined):
    if re.findall(r"#[a-zA-Z0-9_]*", x[3]):
        hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[3])
    # loop through the hashtags and append to list multiple times for each item in tweet (for i in list of items) with a for loop.    
    for i, h in enumerate(hashtags):
        keyword_id += 1
        keyword_list.append([keyword_id, x[0], h])
  
# Creating a dataframe hashtags above
keyword_df = pd.DataFrame(keyword_list, columns=['KeywordID', 'TweetID', 'Keyword'])

# Export dataframes into CSV's
tweets_df1.to_csv('tweetsuk.csv', sep='`', index=False)
tweets_df2.to_csv('tweetsnce.csv', sep='`', index=False)
keyword_df.to_csv('keywords.csv', sep=',', index=False)

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create the tweets table in SQL
cur.execute("""
    CREATE TABLE tweetsuk(
    TweetId bigint PRIMARY KEY,
    Datetime text,
    TwitterHandle text,
    Tweetcontent text
)
""")

#Copy data from csv into the SQL table for tweets
with open('tweetsuk.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'tweetsuk', sep='`')

#Commit the transaction
conn.commit()

# Create the tweetsnce table in SQL
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

# Create the keyword table in SQL
cur.execute("""
    CREATE TABLE keywords(
    KeywordID int PRIMARY KEY,
    TweetId bigint,
    Keyword text
)
""")

#Copy data from csv into the SQL table for keywords
with open('keywords.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'keywords', sep=',')

#Commit the transaction
conn.commit()