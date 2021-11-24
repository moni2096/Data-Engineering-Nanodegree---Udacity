
# Info CryptoSmart
> cryptocurrency, sentiment, AWS, redshift, s3, data engineering, data pipelines
## Project Description
Blockchain & Cryptocurrency has been a hot topic of discussion in past few years. Few experts have already claimed that after the invention of the Web, Blockchain and Cryptocurrency will be the next technology to impact the human species in a significant way. Although, there have been a lot of discussion in the media and another source around the impact some people still do not understand how it will impact their lives. This is my small attempt to build a Cryptocurrency price data pipeline to give data access to anyone who wants to consider investing in Cryptocurrency as well as financial/Cryptocurrency experts to analyze current and historical trends in Cryptocurrency statistics.

## Scope
In this project, we are building a data warehouse in Amazon Redshift by getting Cryptocurrency historical data from [Kaggle](https://www.kaggle.com/tencars/392-crypto-currency-pairs-at-minute-resolution) data source and current statistics around Cryptocurrency from [CoinMarketCap](https://coinmarketcap.com/api/documentation/v1/#) API data and to analyze sentiment and tweets from [Twitter](https://developer.twitter.com/en/docs/twitter-api) API data. The goal is to help the user get the current prices of popular CryptoCurrencies as well as help them analyze historical trends and sentiments of Twitter users around different search terms related to CryptoCurrency.
## Data Collection
| Data        | Source      | Description 
| ----------- | ----------- | ------------
| Crypto Historical Data| https://www.kaggle.com/tencars/392-crypto-currency-pairs-at-minute-resolution|[crypto_historical_data_to_s3.py]() This dataset contains the historical trading data (OHLC) of more than 400 trading pairs at 1-minute resolution reaching back until the year 2013. 
| Twitter API Data| https://developer.twitter.com/en/docs/twitter-api| [twitter_data_to_s3.py]() This data is fetched from Twitter API to get tweets around specific search terms related to CryptoCurrency. We detect the sentiment of tweets using AWS NLP service called [AWS Comprehend](https://aws.amazon.com/comprehend/).
| CointMarket API Data| https://coinmarketcap.com/api/documentation/v1/# |[crypto_price_data_to_s3.py]() This data is fetched from CoinMarketCap API which gives current data around CryptoCurrency price as well as other statistics around CryptoCurrency.
## Data Model
The data model consists of 3 Staging tables, 3 Fact tables, and 3 dimension tables. The tables are modeled using Star Schema. 
![image description](https://github.com/moni2096/Data-Engineering-Nanodegree-Udacity/blob/main/5.%20Capstone%20Project/Schema%20Diagram.png)

#### Table Description
| Table       | Description |
| ----------- | ----------- | 
| stage_tweets| Staging table to store data from twitter|
| stage_crypto_curr_info| Staging table to store latest CryptoCurrency data from CoinMarketCap API
|stage_crypto_historical_info| Staging table to store historical data from Kaggle|
|crypto_historical_info_fact_table| Fact table to store crypto historical data|
|bitcoin_historical_price| Dimension table to store bitcoin data at 1 minute interval from 2013|
|crypto_price_timestamp|Dimension table for all distinct timestamp in Crypto historical information fact table|
|crypto_tweet_fact_table| Fact table to store data from twitter|
|crypto_tweet_user| Dimension table to store data of twitter users from fact table|
|user_sentiment| Dimension table to store data of search term and sentiments of user's tweets|
|crypto_curr_info_fact_table| Fact table to store current information of CryptoCurrency from CoinMakreCap API
|cryptocurrency_top_100| Dimension table to store data from top 100 CryptoCurrency|

> [Data Dictionary.md](https://github.com/moni2096/Data-Engineering-Nanodegree-Udacity/blob/main/5.%20Capstone%20Project/DataDictionary.Md)
## Getting Started
### Steps to run this Project
#### Step 1
Create an IAM role by going to [AWS](https://aws.amazon.com/). The guide for various components inside AWS can be found [here](https://www.dropbox.com/s/ql8wxqjjcv42065/aws-components-setup.pdf?dl=0)

#### Step 2
Create an IAM User by using the same link above and attach **AdministratorAccess** policy to the role.

#### Step 3 
Navigate to current directory in cmd.

#### Step 4
Download user credentials excel and update AWS Access and AWS Secret key in api.cfg. Update the api.cfg file with all the other AWS related configurations.

#### Step 5
Create AWS components using the below command. This can also be done by using the GUI in the guide linked above.
```
python python_scripts/create_aws_components.py
```

#### Step 6
Running above command using python file will return data warehouse end point (DWH_END_POINT) and data warehouse role arn (DWH_ROLE_ARN). Update api.cfg file with the host end point of cluster and IAM role ARN. The api.cfg file should contain all the details that are not mentioned in the file currently.

#### Step 7
Create a [Twitter Developer Account](https://coinmarketcap.com/api/documentation/v1/#section/Introduction) and update the configurations in api.cfg

#### Step 8
Run the twitter_data_to_s3.py file to fetch twitter data corresponding to search terms in the file and load it into S3. Twitter has a daily limit with free version and so we can only fetch attributes from 5000 tweets. Feel free to change the search term in the file depending upon your needs.
```
python python_scripts/twitter_data_to_s3.py
```
#### Step 9
Create a [CoinMarketCap Developer Account](https://coinmarketcap.com/api/documentation/v1/#section/Introduction) account and update configurations in coinmarket_api.yaml.

#### Step 10
Run the crypto_price_data_to_s3.py to fetch data from CoinMarketCap API and load it into S3. 
```
python python_scripts/crypto_price_data_to_s3.py
```
#### Step 11
Upload the historical data from local machine to S3. The file crypto_historical_data_to_s3 also contains preprocessing of this files. Out of 400+ CryptoCurrency for the sake of this project we are only using data from 10 popular CryptoCurrency. **Note**: I have not uploaded the data files to this repository. 
```
python python_scripts/crypto_historical_data_to_s3.py
```

#### Step 13
If the AWS components are created run the below command to create tables by connecting to Postgres database in redshift cluster.
```
python python_scripts/create_tables.py
```
#### Step 12
Run the etl.py file to complete the data load from S3 to Redshift
```
python python_scripts/etl.py
```
#### Step 13
After loading all the tables run crypto_data_quality_checks_and_analysis.ipynb using Jupyter Notebook Launcher to check whether the tables are loaded properly.

#### Step 14
Finally, don't forget to destroy your cluster to avoid additional charges in the future.
```
python python_scripts/destory_aws_components.py
```
#### Project Components
* **dwh.cfg**- The configuration file to store and fetch credentials of AWS components as well as Twitter API keys.
* **twitter_data_to_s3.py**- File to fetch data from Twitter API and store it into S3.
* **crypto_price_data_to_s3.py**- File to fetch data from CoinMaketCap API and store it into S3.
* **crypto_historical_data_to_s3.py**-File to preprocees and upload CryptoCurrency historical data to S3.
* **create_cluster.py** - Used to create cluster, create IAM role and attach policy to the role
* **delete_cluster.py** - To destroy cluster and destroy AWS components.
* **sql_queries.py** - Used to model the staging, fact and dimension tables mentioned in the above section.
* **etl&#46;py** - Used to load raw data from S3 into staging, facts and dimension tables.
* **crypto_data_analysis.ipynb**- Used to test queries and analyze the data from modeled tables.

## Scenarios
- What if data was increased  by 100x.
	- This ETL pipeline is flexible even if the data was increased by 100x. We can always increase the size of Redshift cluster. But the queries and data model is designed by keeping Scalability in mind and so there won't be any issues that we could foresee with increase in size of data models.
- What if the pipeline were run on daily basis by 7 AM?
	- The entire 13 steps as describe above can be scheduled to run  as an [Apache Airflow](https://airflow.apache.org/) or [Prefect](https://www.prefect.io/) Jobs. I plan to do this in my future work and also to bring in more data sources to analyze trends in CryptoCurrency markets.
- What if the database needs to be accessed by 100+ people?
	- The database can be accessed by 100+ people and since it's a Cloud Data Warehouse it would be fairly easy to scale up the resources depending upon the usage of tables as well choosing to go for Normalization as well as Denormalization depending upon usage of end users. In general, increasing resource size would solve the problem, but we can always make additional changes depending upon usage and analyzing cost of queries that are fired.

## Future Work
I am planning to create few Visualizations by connecting to the Redshift tables as well as building Airflow or Prefect pipeline of all the steps involved in running this project.

## Sample Visualizations
* Below are the few sample Visualizations created using the pipeline. The Visualizations were created in [Microsoft Power BI](https://powerbi.microsoft.com/en-us/).

> [image_description](https://github.com/moni2096/Data-Engineering-Nanodegree-Udacity/blob/main/5.%20Capstone%20Project/Price%20Display.png)
> [image_description](https://github.com/moni2096/Data-Engineering-Nanodegree-Udacity/blob/main/5.%20Capstone%20Project/Sentiment%20Analysis.png)
## License
Distributed under the MIT License. See `LICENSE` for more information.











 
