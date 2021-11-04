import os
import pandas as pd
import glob
import configparser
import io
import boto3


class CryptoHistoricalData:

    def __init__(self, config):

        self.config = config
        self.num_tweets = self.config.get("TWEEPY", "NUM TWEETS")

        # AWS Configurations
        self.aws_access_key = self.config.get("AWS", "ACCESS KEY")
        self.aws_secret_access_key = self.config.get("AWS", "ACCESS SECRET")
        self.aws_bucket_name = "udacity-project-crypto-data"

        self.s3_client = boto3.client("s3",
                                      aws_access_key_id=self.aws_access_key,
                                      aws_secret_access_key=self.aws_secret_access_key)

    def save_to_s3(self, df, file_location):

        with io.StringIO() as csv_buffer:

            df.to_csv(csv_buffer, index=False)
            file_location = file_location + f"/{df['currency_name'][0]}.csv"
            response = self.s3_client.put_object(Bucket=self.aws_bucket_name, Key=file_location,
                                                 Body=csv_buffer.getvalue())

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")

    @staticmethod
    def preprocess_files(file, columns_rename_dictionary: dict) -> pd.DataFrame():

        df = pd.read_csv(file, header=0)
        crypto_currency_name = os.path.basename(file).split(".")[0]
        df['currency_name'] = crypto_currency_name
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        df = df.rename(columns_rename_dictionary, axis=1)
        return df


def main():
    path = os.getcwd() + "/crypto_data"
    columns_rename_dictionary = {"time": "price_timestamp", "open": "price_open", "close": "price_close",
                                 "low": "price_low"}

    config = configparser.ConfigParser()
    config.read_file(open('api.cfg'))

    files = glob.glob(path + "/*.csv")

    crypto_historical_data = CryptoHistoricalData(config)

    for filename in files:
        df = crypto_historical_data.preprocess_files(filename, columns_rename_dictionary)
        crypto_historical_data.save_to_s3(df, "crypto_historical_data")


if __name__ == "__main__":
    main()
