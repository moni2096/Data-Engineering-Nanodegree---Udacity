import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('api.cfg')

# DROP TABLES QUERY
stage_tweet_tables_drop = "DROP TABLE IF EXISTS public.stage_tweets"
stage_crypto_curr_info_table_drop = "DROP TABLE IF EXISTS public.stage_crypto_curr_info"
stage_crypto_historical_info_table_drop = "DROP TABLE IF EXISTS public.stage_crypto_historical_prices"
crypto_historical_info_table_drop = "DROP TABLE IF EXISTS crypto_historical_info_fact_table"
bitcoin_historical_price_table_drop = "DROP TABLE IF EXISTS bitcoin_historical_price"
crypto_price_timestamp_table_drop = "DROP TABLE IF EXISTS crypto_price_timestamp"
crypto_tweet_fact_table_drop = "DROP TABLE IF EXISTS crypto_tweet_fact_table"
crypto_tweet_users_drop = "DROP TABLE IF EXISTS crypto_tweet_users"
user_sentiment_drop = "DROP TABLE IF EXISTS user_sentiment"
crypto_curr_info_fact_table_drop = "DROP TABLE IF EXISTS crypto_latest_info_fact_table"
cryptocurrency_top_100_drop = "DROP TABLE IF EXISTS cryptocurrency_top_100"

# CREATE TABLES QUERY

staging_tweets_table_create = ("""CREATE TABLE IF NOT EXISTS public.stage_tweets
                        (tweet_id VARCHAR(256) NOT NULL,
                        tweet_user_id VARCHAR(256) NOT NULL,
                        name VARCHAR(256),
                        nickname VARCHAR(256),
                        user_location VARCHAR(256),
                        followers_count BIGINT,
                        tweets_count BIGINT,
                        user_join_date TIMESTAMP,
                        is_verified BOOLEAN,
                        text VARCHAR(380),
                        likes BIGINT,
                        retweets BIGINT,
                        tweet_date TIMESTAMP,
                        tweet_location VARCHAR(256),
                        source VARCHAR(256),
                        sentiment VARCHAR(50),
                        search_term VARCHAR);
                        """)

staging_crypto_curr_info_table_create = ("""CREATE TABLE IF NOT EXISTS public.stage_crypto_curr_info 
                                (crypto_id INT8,
                                cmc_rank INT4,
                                name VARCHAR,
                                symbol VARCHAR(30),
                                date_added TIMESTAMPTZ,
                                max_supply VARCHAR,
                                num_market_pairs INT8,
                                curr_price NUMERIC,
                                curr_volume_24h NUMERIC,
                                total_supply VARCHAR,
                                circulating_supply NUMERIC);
                            """)

staging_crypto_historical_info_table_create = ("""CREATE TABLE IF NOT EXISTS public.stage_crypto_historical_info_data
                                    (price_timestamp TIMESTAMP, 
                                    price_open NUMERIC,
                                    price_close NUMERIC,
                                    price_high NUMERIC,
                                    price_low NUMERIC,
                                    volume NUMERIC,
                                    currency_name VARCHAR(15));
                                  """)

crypto_historical_info_fact_table_create = ("""CREATE TABLE IF NOT EXISTS crypto_historical_info_fact_table
                                           (crypto_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                                           price_timestamp TIMESTAMP NOT NULL,
                                           price_open NUMERIC,
                                           price_close NUMERIC,
                                           price_high NUMERIC,
                                           price_low NUMERIC,
                                           volume NUMERIC,
                                           currency_name VARCHAR(15));
                                """)

bitcoin_historical_price_table_create = ("""CREATE TABLE IF NOT EXISTS bitcoin_historical_price 
                                        (price_timestamp TIMESTAMP PRIMARY KEY DISTKEY,
                                        price_open NUMERIC,
                                        price_close NUMERIC,
                                        price_high NUMERIC,
                                        price_low NUMERIC,
                                        volume NUMERIC,
                                        currency_name VARCHAR);
                                  """)

crypto_price_timestamp_table_create = ("""CREATE TABLE IF NOT EXISTS crypto_price_timestamp 
                                        (price_timestamp TIMESTAMP PRIMARY KEY DISTKEY,
                                        hours INTEGER,
                                        day INTEGER,
                                        week INTEGER,
                                        month INTEGER,
                                        year INTEGER,
                                        weekday INTEGER);
                                        """)

crypto_tweet_fact_table_create = ("""CREATE TABLE IF NOT EXISTS crypto_tweet_fact_table 
                                (tweet_id VARCHAR(256) PRIMARY KEY NOT NULL,
                                tweet_user_id VARCHAR(256) REFERENCES crypto_tweet_users (tweet_user_id),
                                name VARCHAR(256),
                                nickname VARCHAR(256),
                                user_location VARCHAR(256),
                                followers_count BIGINT,
                                tweets_count BIGINT,
                                user_join_date TIMESTAMP,
                                is_verified BOOLEAN,
                                text VARCHAR(380),
                                likes BIGINT,
                                retweets BIGINT,
                                tweet_date TIMESTAMP,
                                tweet_location VARCHAR(256),
                                source VARCHAR(256),
                                sentiment VARCHAR(50),
                                search_term VARCHAR);
                            """)

crypto_tweet_users_create = ("""CREATE TABLE IF NOT EXISTS crypto_tweet_users
                                (tweet_user_id VARCHAR PRIMARY KEY DISTKEY,
                                name VARCHAR(256),
                                nickname VARCHAR(256),
                                user_location VARCHAR(256),
                                followers_count BIGINT,
                                tweets_count BIGINT,
                                user_join_date TIMESTAMP,
                                is_verified BOOLEAN,
                                source VARCHAR(256));
                            """)

user_sentiment_create = ("""CREATE TABLE IF NOT EXISTS user_sentiment 
                            (tweet_user_id VARCHAR PRIMARY KEY DISTKEY,
                            name VARCHAR(256),
                            user_location VARCHAR(256),
                            followers_count BIGINT,
                            tweets_count BIGINT,
                            tweet_date TIMESTAMP,
                            tweet_location VARCHAR(256),
                            sentiment VARCHAR(50),
                            search_term VARCHAR);
                        """)

crypto_latest_info_fact_table_create = ("""CREATE TABLE IF NOT EXISTS crypto_latest_info_fact_table 
                                        (crypto_id INT8 PRIMARY KEY REFERENCES cryptocurrency_top_100 (crypto_id),
                                        cmc_rank INT4,
                                        name VARCHAR,
                                        symbol VARCHAR(30),
                                        date_added TIMESTAMPTZ,
                                        max_supply VARCHAR,
                                        num_market_pairs INT8,
                                        curr_price NUMERIC,
                                        curr_volume_24h NUMERIC,
                                        total_supply VARCHAR,
                                        circulating_supply NUMERIC);""")

cryptocurrency_top_100_create = ("""CREATE TABLE IF NOT EXISTS cryptocurrency_top_100 
                                    (crypto_id INT8 PRIMARY KEY,
                                    cmc_rank INT4,
                                    name VARCHAR,
                                    symbol VARCHAR(30),
                                    curr_price NUMERIC,
                                    curr_volume_24h NUMERIC,
                                    total_supply VARCHAR);
                                    """)

staging_tweets_copy = ("""COPY stage_tweets FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        COMPUPDATE OFF region 'us-west-2'
                        TIMEFORMAT AS 'auto'
                        FORMAT AS JSON 'auto';
                        """.format(config.get('S3', 'TWITTER_DATA'),
                                   config.get('IAM_ROLE', 'ARN')))

staging_crypto_curr_info_copy = ("""COPY stage_crypto_curr_info FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'auto'
                            FORMAT AS JSON 'auto';
                            """).format(config.get('S3', 'CRYPTO_INFO_DATA'),
                                        config.get('IAM_ROLE', 'ARN'))

staging_crypto_historical_info_copy = ("""COPY stage_crypto_historical_info_data FROM {}
                                    CREDENTIALS 'aws_iam_role={}'
                                    COMPUPDATE OFF region 'us-west-2'
                                    TIMEFORMAT AS 'auto'
                                    IGNOREHEADER 1
                                    DELIMITER AS ','
                                    FORMAT AS CSV;
                                    """).format(config.get('S3', 'CRYPTO_HISTORICAL_DATA'),
                                                config.get('IAM_ROLE', 'ARN'))

crypto_historical_info_fact_table_copy = ("""INSERT INTO crypto_historical_info_fact_table (price_timestamp, price_open, price_close, price_high, price_low, volume, currency_name)
                                        SELECT price_timestamp, 
                                        price_open, 
                                        price_close, 
                                        price_high, 
                                        price_low, 
                                        volume, 
                                        currency_name 
                                        FROM public.stage_crypto_historical_info_data
                                        WHERE price_timestamp IS NOT NULL;
                                    """)

bitcoin_historical_price_table_copy = ("""INSERT INTO bitcoin_historical_price (price_timestamp, price_open, price_close, price_high, price_low, volume, currency_name)
                                        SELECT DISTINCT(price_timestamp), 
                                        price_open, 
                                        price_close, 
                                        price_high, 
                                        price_low, 
                                        volume, 
                                        currency_name 
                                        FROM public.stage_crypto_historical_info_data a WHERE a.currency_name = 'btcusd'
                                        AND price_timestamp IS NOT NULL;
                                        """)

crypto_price_timestamp_table_copy = ("""INSERT INTO crypto_price_timestamp (price_timestamp, hours, day, week, month, year, weekday)
                                        SELECT DISTINCT(price_timestamp),
                                        EXTRACT(hour from price_timestamp),
                                        EXTRACT(day from price_timestamp),
                                        EXTRACT(week from price_timestamp),
                                        EXTRACT(month from price_timestamp),
                                        EXTRACT(year from price_timestamp),
                                        EXTRACT(weekday from price_timestamp)
                                        FROM public.stage_crypto_historical_info_data
                                        WHERE price_timestamp IS NOT NULL;""")


crypto_tweet_fact_table_copy = ("""INSERT INTO crypto_tweet_fact_table (tweet_id, tweet_user_id, name, nickname, user_location, followers_count, tweets_count, user_join_date,
                                is_verified, text, likes, retweets, tweet_date, tweet_location, source, sentiment, search_term)
                                SELECT DISTINCT(tweet_id),
                                tweet_user_id,
                                name,
                                nickname,
                                user_location,
                                followers_count,
                                tweets_count,
                                user_join_date,
                                is_verified,
                                text,
                                likes,
                                retweets,
                                tweet_date,
                                tweet_location,
                                source,
                                sentiment,
                                search_term FROM public.stage_tweets
                            """)

crypto_tweet_users_copy = ("""INSERT INTO crypto_tweet_users (tweet_user_id, name, nickname, user_location, followers_count, tweets_count, user_join_date, is_verified, source)
                              SELECT DISTINCT(tweet_user_id),
                                name,
                                nickname,
                                user_location,
                                followers_count,
                                tweets_count,
                                user_join_date,
                                is_verified,
                                source FROM public.stage_tweets
                            """)

user_sentiment_copy = ("""INSERT INTO user_sentiment (tweet_user_id, name, user_location, followers_count, tweets_count, tweet_date, tweet_location, sentiment, search_term)
                         SELECT DISTINCT(tweet_user_id),
                          name,
                          user_location,
                          followers_count,
                          tweets_count,
                          tweet_date,
                          tweet_location,
                          sentiment,
                          search_term FROM public.stage_tweets
                       """)

crypto_latest_info_fact_table_copy = ("""INSERT INTO crypto_latest_info_fact_table (crypto_id, cmc_rank, name, symbol, date_added, max_supply,
                                        num_market_pairs, curr_price, curr_volume_24h, total_supply, circulating_supply)
                                        SELECT crypto_id,
                                        cmc_rank,
                                        name,
                                        symbol,
                                        date_added,
                                        max_supply,
                                        num_market_pairs,
                                        curr_price,
                                        curr_volume_24h,
                                        total_supply,
                                        circulating_supply
                                        FROM public.stage_crypto_curr_info;
                                    """)

cryptocurrency_top_100_copy = ("""INSERT INTO cryptocurrency_top_100 (crypto_id, cmc_rank, name, symbol, curr_price, curr_volume_24h, total_supply)
                                 SELECT crypto_id,
                                  cmc_rank,
                                  name,
                                  symbol,
                                  curr_price,
                                  curr_volume_24h,
                                  total_supply
                                FROM public.stage_crypto_curr_info
                                WHERE cmc_rank <= 100;
                               """)


drop_table_queries = [stage_tweet_tables_drop, stage_crypto_curr_info_table_drop,
                      stage_crypto_historical_info_table_drop, crypto_historical_info_table_drop,
                      crypto_price_timestamp_table_drop, crypto_tweet_fact_table_drop, crypto_tweet_users_drop,
                      user_sentiment_drop, crypto_curr_info_fact_table_drop, cryptocurrency_top_100_drop]

create_table_queries = [staging_tweets_table_create, staging_crypto_curr_info_table_create,
                        staging_crypto_historical_info_table_create, crypto_historical_info_fact_table_create, bitcoin_historical_price_table_create,
                        crypto_price_timestamp_table_create, crypto_tweet_users_create, crypto_tweet_fact_table_create, user_sentiment_create,
                        cryptocurrency_top_100_create, crypto_latest_info_fact_table_create]

copy_table_queries = [staging_tweets_copy, staging_crypto_curr_info_copy, staging_crypto_historical_info_copy,
                      crypto_historical_info_fact_table_copy, bitcoin_historical_price_table_copy, crypto_price_timestamp_table_copy, crypto_tweet_fact_table_copy,
                      crypto_tweet_users_copy, user_sentiment_copy, crypto_latest_info_fact_table_copy, cryptocurrency_top_100_copy]



