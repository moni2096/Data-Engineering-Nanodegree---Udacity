from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import yaml
import configparser
import s3fs


class CoinMarketCapAPISearch:

    def __init__(self, config):

        # API Configurations
        self.session_headers = config['DEFAULT']['SESSION_HEADERS']
        self.url_parameters = config['DEFAULT']['URL_PARAMETERS']
        self.url = config['DEFAULT']['URL']

        # AWS Configurations
        self.aws_bucket_name = "udacity-project-crypto-data"
        self.s3_file_system = s3fs.S3FileSystem(anon=False)

    @staticmethod
    def get_required_features(coin_data_object: json) -> dict:

        required_features = {
            "crypto_id": coin_data_object['id'],
            "cmc_rank": coin_data_object['cmc_rank'],
            "name": coin_data_object['name'],
            "symbol": coin_data_object['symbol'],
            "date_added": coin_data_object['date_added'],
            "max_supply": coin_data_object['max_supply'],
            "num_market_pairs": coin_data_object['num_market_pairs'],
            "curr_price": coin_data_object['quote']['USD']['price'],
            "curr_volume_24h": coin_data_object['quote']['USD']['volume_24h'],
            "total_supply": coin_data_object['total_supply'],
            "circulating_supply": coin_data_object['circulating_supply']
        }
        return required_features

    def send_data_request_store_to_s3(self):

        session = Session()
        session.headers.update(self.session_headers)

        if int(self.url_parameters['limit']) > 5000:
            raise ValueError('API rate limit reached. Please enter limit less than 5000')
        try:
            response = session.get(self.url, params=self.url_parameters)
            data = json.loads(response.text)
            crypto_currency_coins_data = data['data']
            for coin_data in crypto_currency_coins_data:
                filtered_data = self.get_required_features(coin_data)
                file_location = self.aws_bucket_name + "/api_data" + "/crypto_price_data_json"
                with self.s3_file_system.open(file_location + f"/crypto_data_{coin_data['id']}.json", 'w') as f:
                    json.dump(filtered_data, f)

        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)


def main():
    with open('coinmarket_api.yaml') as c:
        coin_market_config = yaml.load(c, Loader=yaml.FullLoader)

    aws_config = configparser.ConfigParser()
    aws_config.read_file(open('api.cfg'))

    crypto_data_search = CoinMarketCapAPISearch(coin_market_config)

    crypto_data_search.send_data_request_store_to_s3()


if __name__ == "__main__":
    main()
