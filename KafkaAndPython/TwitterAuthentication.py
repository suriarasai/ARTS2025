import tweepy
import time

# Twitter API credentials
api_key = "XXXX"
api_secret_key = "XXXX"
access_token = "XXXX"
access_token_secret = "XXXX"
bearer_token = "%XXXX"

# Authenticate to Twitter
auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)
# Create API object
api = tweepy.API(auth)

print("Authentication OK")
client = tweepy.Client(bearer_token, api_key, api_secret_key, access_token, access_token_secret)


# User's username to fetch tweets from
username = '@ScienceGuys'  # Replace 'twitter_username' with the actual username

# Fetch the latest 10 tweets from the user's timeline
tweets = api.user_timeline(screen_name=username, count=10)

# Print each tweet's text
for tweet in tweets:
    print(tweet.text)