import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar,
    status integer,
    ts varchar,
    userAgent varchar,
    userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint IDENTITY(0,1) sortkey distkey,
    start_time varchar not null, 
    user_id integer not null, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id integer, 
    location varchar, 
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id integer not null sortkey, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year integer, 
    duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id  varchar PRIMARY KEY,
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP  PRIMARY KEY, 
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer, 
    weekday varchar
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {} 
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2' 
FORMAT AS JSON {}; 
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'].strip('"\''), config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {} 
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2' 
JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'].strip('"\''))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
(
SELECT 
    e.ts,
    e.userid,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid,
    e.location,
    e.useragent
FROM staging_events e 
JOIN staging_songs s
ON  (e.song = s.title AND e.artist = s.artist_name)   
)
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name, 
    last_name, 
    gender, 
    level
)
(
SELECT DISTINCT 
    userid, 
    firstname, 
    lastname, 
    gender, 
    level 
FROM staging_events 
WHERE userid IS NOT NULL
)
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
) 
(
SELECT DISTINCT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration 
FROM staging_songs
)
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name, 
    location, 
    latitude, 
    longitude
) 
(
SELECT DISTINCT 
    artist_id, 
    artist_name,
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
)
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour, 
    day, 
    week, 
    month,
    year,
    weekday
) 
(
SELECT DISTINCT 
    TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second' as start_time,
    EXTRACT(HOUR FROM TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second') as hour,
    EXTRACT(DAY FROM TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second') as day,
    EXTRACT(WEEK FROM TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second') as week,
    EXTRACT(MONTH FROM TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second') as month, 
    EXTRACT(YEAR FROM TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second') as year,
    EXTRACT(DOW FROM TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second') as weekday
FROM staging_events
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
