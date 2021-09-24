import snscrape.modules.twitter as sntwitter
import pandas as pd
import re

# Creating list to append tweet data to
tweets_list = []

#List of hashtags to search
hashtags_list = [
'#Innovation'
]
orString = ' OR '
hashtags_list_or = orString.join(hashtags_list)

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper(hashtags_list_or + ' since:2021-01-01 until:2021-12-31').get_items()):
    if i>400:
        break
    tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.user.location, tweet.media, tweet.outlinks, tweet.lang, tweet.source, tweet.mentionedUsers])

#Taking the list of hashtags from the tweet strings
for i, x in enumerate(tweets_list):
    hashtags = re.findall(r"#[a-zA-Z0-9_]*", x[2])
    print(x[1], hashtags)

# # Creating a dataframe from the tweets list above
tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username', 'Location', 'Media', 'Outlinks', 'Language', 'Source', 'MentionedUsers'])

# # Export dataframe into a CSV
tweets_df.to_csv('tweets.csv', sep=',', index=False)