import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, hour
from pyspark.sql.types import IntegerType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Function to initiate the Spark sesssion
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to create song and artist table and to process song data
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(output_data + "/songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = (df.select('artist_id',
                               col('artist_name').alias('name'),
                               col('artist_location').alias('location'),
                               col('artist_latitude').alias('latitude'),
                               col('artist_longitude').alias('longitude')).distinct()
                     )

    # write artists table to parquet files"
    artists_table.write.parquet(output_data + "/artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Function to create users, time, and songplays table and to process log data
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = (df.select(col('userId').alias('user_id'),
                             col('firstName').alias('first_name'),
                             col('lastName').alias('last_name'),
                             col('gender').alias('gender'),
                             col('level').alias('level')).distinct()
                   )

    # write users table to parquet files
    users_table.write.parquet(output_data + "/users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.from_unixtime(x))
    df = df.withColumn('date_time', F.from_unixtime('start_time'))

    # extract columns to create time table
    time_table = (df.select(col('start_time'),
                            hour('date_time').alias('hour'),
                            dayofmonth('date_time').alias('day'),
                            weekofyear('date_time').alias('week'),
                            month('date_time').alias('month'),
                            year('date_time').alias('year'),
                            date_format('date_time', 'E').alias('weekday'))
                  )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(output_data + '/time.parquet', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + '/songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        df.withColumn('songplay_id', F.monotonically_increasing_id())
            .join(song_df, song_df.title == df.song)
            .select('songplay_id',
                    col('ts').alias('start_time'),
                    col('userId').alias('user_id'),
                    col('level').alias('level'),
                    col('song_id').alias('song_id'),
                    col("artist_id").alias('artist_id'),
                    col('sessionId').alias('session_id'),
                    col('location').alias('location'),
                    col('userAgent').alias('user_agent'),
                    year('date_time').alias('year'),
                    month('date_time').alias('month'))
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(output_data + "/songplays.parquet", mode='overwrite')


def main():
    """
    Main function which calls all the function to process data and store it into output S3 bucket
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-data-lake-output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
