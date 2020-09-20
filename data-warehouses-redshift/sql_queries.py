import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# Note: CASCADE specifies when a referenced row is deleted, row(s) referencing it should be automatically deleted also
staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users CASCADE;"
song_table_drop = "DROP table IF EXISTS songs CASCADE;"
artist_table_drop = "DROP table IF EXISTS artists CASCADE;"
time_table_drop = "DROP table IF EXISTS time CASCADE;"

# CREATE TABLES
staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events(artist varchar, \
                                                                         auth varchar, \
                                                                         firstName varchar, \
                                                                         gender char(1), \
                                                                         itemInSession int, \
                                                                         lastName varchar, \
                                                                         length decimal, \
                                                                         level varchar(10), \
                                                                         location varchar, \
                                                                         method varchar(10), \
                                                                         page varchar, \
                                                                         registration decimal, \
                                                                         sessionId int, \
                                                                         song varchar, \
                                                                         status int, \
                                                                         ts bigint, \
                                                                         userAgent varchar, \
                                                                         userId int);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs(num_songs int, \
                                                                        artist_id varchar, \
                                                                        artist_latitude decimal, \
                                                                        artist_longitude decimal, \
                                                                        artist_location varchar, \
                                                                        artist_name varchar, \
                                                                        song_id varchar, \
                                                                        title varchar, \
                                                                        duration decimal, \
                                                                        year int);")
# Note: 
# 1. Since SERIAL command in Postgres is not supported in Redshift, I need to use IDENTITY(0,1) to generate identity column instead
# 2. I reference other dimension tables by referencing some foreign keys
songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint IDENTITY(0,1), \
                                                                start_time bigint REFERENCES time (start_time), \
                                                                user_id int REFERENCES users (user_id), \
                                                                level varchar(10), \
                                                                song_id varchar REFERENCES songs (song_id), \
                                                                artist_id varchar REFERENCES artists (artist_id), \
                                                                session_id int NOT NULL, \
                                                                location varchar, \
                                                                user_agent varchar);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, \
                                                        first_name varchar NOT NULL, \
                                                        last_name varchar NOT NULL, \
                                                        gender char(1), \
                                                        level varchar(10));")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, \
                                                        title varchar NOT NULL, \
                                                        artist_id varchar NOT NULL, \
                                                        year int, \
                                                        duration decimal);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, \
                                                            name varchar NOT NULL, \
                                                            location varchar, \
                                                            latitude decimal, \
                                                            longitude decimal);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, \
                                                       hour int NOT NULL, \
                                                       day int NOT NULL, \
                                                       week int NOT NULL, \
                                                       month int NOT NULL, \
                                                       year int NOT NULL, \
                                                       weekday int NOT NULL);")

# STAGING TABLES

# Get the AWS configurations from dwh.cfg file
DWH_ROLE_ARN=config.get('IAM_ROLE','ARN')
S3_LOG_DATA=config.get('S3','LOG_DATA')
S3_LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
S3_SONG_DATA=config.get('S3','SONG_DATA')

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    format as json {} 
    compupdate off 
    region 'us-west-2';
""").format(S3_LOG_DATA,DWH_ROLE_ARN,S3_LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs 
    from {}
    iam_role {}
    format as json 'auto' 
    compupdate off 
    region 'us-west-2';
""").format(S3_SONG_DATA,DWH_ROLE_ARN)

# FINAL TABLES

# Since I can't use ON CONFLICT on Redshift, I need to use DISTINCT with some conditions to make sure there is no duplicate data
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT events.ts,
                     events.userId,
                     events.level,
                     songs.song_id,
                     songs.artist_id,
                     events.sessionId,
                     events.location,
                     events.userAgent 
    FROM staging_events AS events
    JOIN staging_songs AS songs
    ON (events.artist = songs.artist_name)
    AND (events.song = songs.title)
    AND (events.length = songs.duration)
    WHERE events.page = 'NextSong';
""")

# Select DISTINCT and also prevent not to insert the duplicate primary key in the WHERE clause
# We also consider only records associated with page='NextSong'
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong' AND
          userId NOT IN (SELECT DISTINCT user_id FROM users);
""")

# Select DISTINCT and also prevent not to insert the duplicate primary key in the WHERE clause
song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration::decimal
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
    
""")

# Select DISTINCT and also prevent not to insert the duplicate primary key in the WHERE clause
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude::decimal,
                    artist_longitude::decimal
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

# Select DISTINCT and also prevent not to insert the duplicate primary key in the WHERE clause
# Note: I didn't filtered only records associated with page='NextSong' but instead pull all pages's records
#       since I think this time table was served as a lookup table so it should hold all the timestamp records
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT events.start_time,
                    EXTRACT (HOUR FROM events.start_time), 
                    EXTRACT (DAY FROM events.start_time),
                    EXTRACT (WEEK FROM events.start_time), 
                    EXTRACT (MONTH FROM events.start_time),
                    EXTRACT (YEAR FROM events.start_time), 
                    EXTRACT (WEEKDAY FROM events.start_time) 
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) events
    WHERE events.start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
# Note: I move 'songplay_table_insert' to the last since songplays table reference other tables as the foreign keys
#       Therefore, other tables's records need to exists first otherwise songplay won't find any records from other tables when doing join
#       and will result in 0 record after insert finish
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
