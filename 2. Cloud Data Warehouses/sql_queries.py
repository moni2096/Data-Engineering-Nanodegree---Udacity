import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist VARCHAR,
                                                            auth VARCHAR,
                                                            firstName VARCHAR,
                                                            gender VARCHAR,
                                                            itemInSession INTEGER,
                                                            lastName VARCHAR,
                                                            length FLOAT8,
                                                            level VARCHAR,
                                                            location VARCHAR, 
                                                            method VARCHAR,
                                                            page VARCHAR,
                                                            registration BIGINT,
                                                            sessionId INTEGER,
                                                            song VARCHAR, 
                                                            status INT,
                                                            ts TIMESTAMP,
                                                            userAgent VARCHAR, 
                                                            userId INTEGER);
                                """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs INTEGER,
                                                            artist_id VARCHAR,
                                                            artist_latitude FLOAT8,
                                                            artist_longitude FLOAT8,
                                                            artist_location VARCHAR,
                                                            artist_name VARCHAR,
                                                            song_id VARCHAR,
                                                            title VARCHAR,
                                                            duration FLOAT8,
                                                            year INT);
                                                            
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
                                                         start_time TIMESTAMP NOT NULL, 
                                                         user_id INTEGER NOT NULL REFERENCES users (user_id),
                                                         level VARCHAR(8),
                                                         song_id VARCHAR REFERENCES songs (song_id),
                                                         artist_id VARCHAR REFERENCES artists (artist_id),
                                                         session_id INTEGER, 
                                                         location VARCHAR(MAX),
                                                         user_agent VARCHAR);   
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY distkey,
                                                        first_name VARCHAR(25),
                                                        last_name VARCHAR(25),
                                                        gender VARCHAR(1),
                                                        level VARCHAR(8));
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR PRIMARY KEY,
                                                    title VARCHAR(MAX),
                                                    artist_id VARCHAR(50) distkey,
                                                    year INTEGER,
                                                    duration FLOAT8);
                    """)


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR PRIMARY KEY distkey,
                                                    name VARCHAR(MAX),
                                                    location VARCHAR(MAX),
                                                    latitude FLOAT8,
                                                    longitude FLOAT8);
                     """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP PRIMARY KEY distkey,
                                                    hour INTEGER,
                                                    day INTEGER,
                                                    week INTEGER,
                                                    month INTEGER,
                                                    year INTEGER,
                                                    weekday INTEGER); 
                    """)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        COMPUPDATE OFF region 'us-west-2'
                        TIMEFORMAT as 'epochmillisecs'
                        FORMAT AS JSON {};
                        """).format(config.get('S3', 'LOG_DATA'), 
                                    config.get('IAM_ROLE', 'ARN'), 
                                    config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = ("""COPY staging_songs FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        COMPUPDATE OFF region 'us-west-2'
                        JSON 'auto' TRUNCATECOLUMNS;
                        """).format(config.get('S3', 'SONG_DATA'), 
                                    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             SELECT DISTINCT(e.ts) AS start_time, 
                             e.userId AS user_id, 
                             e.level AS level, 
                             s.song_id AS song_id, 
                             s.artist_id AS artist_id, 
                             e.sessionId AS session_id, 
                             e.location AS location, 
                             e.userAgent AS user_agent
                             FROM staging_events e JOIN staging_songs s 
                             ON e.song = s.title AND e.artist = s.artist_name
                             AND e.page = 'NextSong';
                        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userID AS user_id, 
                        firstName AS first_name, 
                        lastName AS last_name, 
                        gender AS gender, 
                        level AS level
                        FROM staging_events WHERE user_id IS NOT NULL;
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id AS song_id, 
                        title AS title, 
                        artist_id AS artist_id, 
                        year AS year, 
                        duration AS duration 
                        FROM staging_songs WHERE song_id IS NOT NULL;
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                         SELECT DISTINCT artist_id AS artist_id,
                          artist_name AS name,
                          artist_location AS location,
                          artist_latitude AS latitude,
                          artist_longitude AS longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
                    """)



time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT distinct ts,
                        EXTRACT(hour from ts),
                        EXTRACT(day from ts),
                        EXTRACT(week from ts),
                        EXTRACT(month from ts),
                        EXTRACT(year from ts),
                        EXTRACT(weekday from ts)
                        FROM staging_events
                        WHERE ts IS NOT NULL;
                   """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analysis_queries = [song_play_analysis, user_analysis, song_analysis, artist_analysis, time_analyis]
