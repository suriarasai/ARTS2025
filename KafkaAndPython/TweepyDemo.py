import tweepy

# Replace with your actual Bearer Token
bearer_token = "AAAAAAAAAAAAAAAAAAAAACFGuAEAAAAAQgCaIrW9%2FBzAaCPPfo4kn9kp78k%3DHxXsgDwMkBuWqQyOyg6NbHprMrsUpR2hqNmdlmMHnEAjU4qTRw"

client = tweepy.Client(bearer_token=bearer_token)

# Define the query. This query searches for tweets containing "#Python" in English, excluding retweets.
query = "#HDB -is:retweet lang:en"

# Maximum number of tweets to collect
max_results = 2

# Search for recent tweets
response = client.search_recent_tweets(query=query, max_results=max_results)

if response.data:
    for tweet in response.data:
        print(tweet.text)
else:
    print("No tweets found.")
