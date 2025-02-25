import tweepy

# Replace with your actual Bearer Token
bearer_token = "XXXX"

client = tweepy.Client(bearer_token=bearer_token)

# Define the query. This query searches for tweets containing "#Python" in English, excluding retweets.
query = "#earthquake -is:retweet lang:en"

# Maximum number of tweets to collect
max_results = 2

# Search for recent tweets
response = client.search_recent_tweets(query=query, max_results=max_results)

if response.data:
    for tweet in response.data:
        print(tweet.text)
else:
    print("No tweets found.")
