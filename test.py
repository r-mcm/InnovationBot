import snscrape.modules.twitter as sntwitter
import pandas as pd
import re

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
    if i>3:
        break
    tweets_list1.append([tweet.id, tweet.date, tweet.user.username, tweet.content.replace('\n','')])

#Taking the list of hashtags from the tweet strings
# for item, items in enumerate(tweets_list1):

for i, x in enumerate(tweets_list1):
    hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[3])
    print(x[1], hashtags)

print(tweets_list1)