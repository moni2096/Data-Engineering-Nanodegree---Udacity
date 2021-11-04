import tweepy
import configparser
import datetime
from botocore.exceptions import ClientError
import boto3
import json
import s3fs


class TweetSearch:

    def __init__(self, config):

        self.config = config
        self.num_tweets = self.config.get("TWEEPY", "NUM TWEETS")

        # AWS Configurations
        self.aws_access_key = self.config.get("AWS", "ACCESS KEY")
        self.aws_secret_access_key = self.config.get("AWS", "ACCESS SECRET")
        self.aws_bucket_name = "udacity-project-crypto-data"
        self.comprehend_client = boto3.client("comprehend",
                                              aws_access_key_id=self.aws_access_key,
                                              aws_secret_access_key=self.aws_secret_access_key)
        self.s3_client = boto3.client("s3",
                                      aws_access_key_id=self.aws_access_key,
                                      aws_secret_access_key=self.aws_secret_access_key)
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

        # API Configurations
        self.consumer_key = self.config.get('API', 'KEY')
        self.consumer_secret = self.config.get('API', 'SECRET')

        self.access_token = self.config.get('API', 'ACCESS TOKEN')
        self.access_secret = self.config.get('API', 'ACCESS SECRET')

        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_secret)

    def get_required_features(self, tweet: dict) -> dict:

        tweet_required_features = {
            "tweet_id": tweet.id_str,
            "tweet_user_id": tweet.user.id,
            "name": tweet.user.name,
            "nickname": tweet.user.screen_name,
            "user_location": tweet.user.location,
            "followers_count": tweet.user.followers_count,
            "tweets_count": tweet.user.statuses_count,
            "user_join_date": tweet.user.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "is_verified": tweet.user.verified,
            "text": tweet.text,
            "likes": tweet.favorite_count,
            "retweets": tweet.retweet_count,
            "tweet_date": tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "tweet_location": tweet.place.full_name if tweet.place else None,
            "source": tweet.source,
            "sentiment": self.get_sentiment(tweet.text, tweet.lang)
        }
        return tweet_required_features

    def get_sentiment(self, text: str, lang: str):

        try:
            sentiment = self.comprehend_client.detect_sentiment(Text=text, LanguageCode=lang)
        except ClientError as ex:
            return "Unable to detect sentiment in this language"
        return sentiment["Sentiment"]

    def query_tweets(self, search_terms: list):

        api = tweepy.API(self.auth)
        for term in search_terms:
            public_tweets = api.search(q=f"{term}", until=(datetime.date.today() - datetime.timedelta(1)).isoformat(),
                                       count=self.num_tweets)

            for tweet in public_tweets:
                filtered_tweet = self.get_required_features(tweet)
                filtered_tweet["search_term"] = term
                file_location = self.aws_bucket_name + "/api_data" + "/tweets_json_data"
                with self.s3_file_system.open(file_location + f"/tweet_data_{tweet.id_str}.json", 'w') as f:
                    json.dump(filtered_tweet, f)


def main():
    config = configparser.ConfigParser()
    config.read_file(open('api.cfg'))
    tweet_miner = TweetSearch(config)

    search_terms = ['Bitcoin', 'Ethereum', 'Tether', 'ADA', 'USDT', 'Tether', 'Cardano', 'Crypto',  'Cryptocurrency', 'Coin', 'ETH', 'BTC', 'Crypto Market', 'Coinbase', 'Crypto Currency',
                    'Crypto Currency Investor', 'Crypto Currency Tracker', 'Crypto Currency Investment', 'Bitcoin Investment', 'Ethereum Investment']

    tweet_miner.query_tweets(search_terms)


if __name__ == "__main__":
    main()
