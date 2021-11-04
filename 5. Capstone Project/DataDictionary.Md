**1) Staging Tables**
* Table Name: stage_tweets
    | Column Name   | Data Type     |
    | ------------- | ------------- |
    | tweet_id 	 | VARCHAR(256) |
    | tweet_user_id| VARCHAR(256) |
    | name| VARCHAR(256)  |
    | nickname| VARCHAR(256)|
    | user_location| VARCHAR(256) |
    | followers_count| BIGINT  |
    | tweets_count| BIGINT|
    | user_join_date| TIMESTAMP|
    |is_verified| BOOLEAN|
    |text| VARCHAR(380)|
    |likes| BIGINT|
    |retweets| BIGINT|
    |tweet_date| TIMESTAMP|
    |tweet_location| VARCHAR(256)|
    |source| VARCHAR(256)|
    |sentiment| VARCHAR(50)|
    |search_term| VARCHAR|
* Table Name: stage_crypto_curr_info

	 | Column Name   | Data Type     |
	 | ------------- | ------------- |
	 | crypto_id | INT8 |
	 | cmc_rank | INT4 |
	 | name | VARCHAR |
	 | symbol | VARCHAR(30)|
	 | date_added | TIMESTAMPTZ|
	 | max_supply | VARCHAR |
	 | num_market_pairs | INT8|
	 | curr_price | NUMERIC |
	 | curr_volume_24h | NUMERIC |
	 | total_supply | VARCHAR |
	 | circulating_supply | NUMERIC |
* Table Name: crypto_historical_info_fact_table

	 | Column Name |  Data Type |
     | ------------- | ------------- |
     | price_timestamp | TIMESTAMP|
     | price_open | NUMERIC|
     | price_close| NUMERIC |
     |price_high | NUMERIC |
     | price_low | NUMERIC |
     | volume | NUMERIC |
     | currency_name| VARCHAR(15)|

**2) Fact Tables**
* Table Name: crypto_historical_info_fact_table

    | Column Name   | Data Type     |
    | ------------- | ------------- |
    | crypto_id | INTEGER IDENTIY PRIMARY KEY|
    | price_timestamp | TIMESTAMP|
     | price_open | NUMERIC|
     | price_close| NUMERIC |
     |price_high | NUMERIC |
     | price_low | NUMERIC |
     | volume | NUMERIC |
     | currency_name| VARCHAR(15)|

* Table Name: crypto_tweet_fact_table
    | Column Name   | Data Type     |
    | ------------- | ------------- |
     | tweet_id 	 | VARCHAR(256) PRIMARY KEY|
     | tweet_user_id| VARCHAR(256) |
     | name| VARCHAR(256)  |
     | nickname| VARCHAR(256)|
     | user_location| VARCHAR(256) |
     | followers_count| BIGINT  |
     | tweets_count| BIGINT|
     | user_join_date| TIMESTAMP|
     |is_verified| BOOLEAN|
     |text| VARCHAR(380)|
     |likes| BIGINT|
     |retweets| BIGINT|
     |tweet_date| TIMESTAMP|
     |tweet_location| VARCHAR(256)|
     |source| VARCHAR(256)|
     |sentiment| VARCHAR(50)|
     |search_term| VARCHAR|
     
 * Table Name: crypto_latest_info_fact_table
    | Column Name   | Data Type     |
	 | ------------- | ------------- |
	 | crypto_id | INT8 PRIMARY KEY|
	 | cmc_rank | INT4 |
	 | name | VARCHAR |
	 | symbol | VARCHAR(30)|
	 | date_added | TIMESTAMPTZ|
	 | max_supply | VARCHAR |
	 | num_market_pairs | INT8|
	 | curr_price | NUMERIC |
	 | curr_volume_24h | NUMERIC |
	 | total_supply | VARCHAR |
	 | circulating_supply | NUMERIC|
	 
**3) Dimension Tables**

* Table Name: bitcoin_historical_price
  | Column Name | Data Type |
  |--------------|-----------|
   | price_timestamp| TIMESTAMP|
   | price_open | NUMERIC |
   | price_close | NUMERIC |
   | price_high | NUMERIC |
   | price_low | NUMERIC |
   | volume | NUMERIC |
   | currency_name| VARCHAR(15)|
   
* Table Name: crypto_price_timestamp

  | Column Name | Data Type |
  |--------------|-----------|
   | price_timestamp| TIMESTAMP|
   | hours | INTEGER |
   | day | INTEGER|
   | week| INTEGER|
   | month| INTEGER|
   | year  | INTEGER|
   | weekday| INTEGER|

* Table Name: crypto_tweet_user_create
  | Column Name   | Data Type     |
    | ------------- | ------------- |
	| tweet_id 	 | VARCHAR(256) PRIMARY KEY DISTKEY |
	| name | VARCHAR(256)|
	|nickname | VARCHAR(256)|
	| user_location| VARCHAR(256)|
	|followers_count | BIGINT|
	|tweets_count | BIGINT | 
	|user_join_date | TIMESTAMP| 
	|is_verified | BOOLEAN|  
	|source | VARCHAR(256)|

* Table Name: user_sentiment_create
   | Column Name   | Data Type     |
    | ------------- | ------------- |
	| tweet_id 	 | VARCHAR(256) PRIMARY KEY DISTKEY |
	| name | VARCHAR(256)|
	|nickname | VARCHAR(256)|
	| user_location| VARCHAR(256)|
	|followers_count | BIGINT|
	|tweets_count | BIGINT | 
	|tweet_date| TIMESTAMP| 
	|tweet_location| VARCHAR(256)|  
	|sentiment| VARCHAR(50)|
	|search_term| VARCHAR|

* Table Name: crypto_top_100
	| Column Name   | Data Type     |
	 | ------------- | ------------- |
	 | crypto_id | INT8 PRIMARY KEY |
	 | cmc_rank | INT4 |
	 | name | VARCHAR |
	 | curr_price | NUMERIC |
	 | curr_volume_24h | NUMERIC |
	 | total_supply | VARCHAR |
	
	

	
	
	

	




