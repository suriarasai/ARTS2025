import tweepy

# Replace with your actual Bearer Token from your Twitter Developer account
bearer_token = "XXX"

# Initialize the Tweepy client
client = tweepy.Client(bearer_token=bearer_token)

# Replace 'exampleuser' with the target username
username = "narendramodi"

# Get user details by username
user_response = client.get_user(username=username)

if user_response.data:
    user = user_response.data
    print("User ID:", user.id)
    print("Name:", user.name)
    print("Username:", user.username)
else:
    print("User not found.")

if user_response.data:
    user_id = user_response.data.id
    # Retrieve up to 10 recent tweets from the user's timeline
    tweets_response = client.get_users_tweets(id=user_id, max_results=10)

    if tweets_response.data:
        print(f"Recent tweets by @{username}:")
        for tweet in tweets_response.data:
            print(f"- {tweet.text}")
    else:
        print("No tweets found for this user.")
else:
    print("User not found.")