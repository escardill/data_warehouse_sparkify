import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar not null,
    first_name varchar not null,
    gender varchar(1) not null,
    item_in_session integer,
    last_name varchar not null,
    length float,
    level varchar(5) not null,
    location varchar not null,
    method varchar(5) not null,
    page varchar not null,
    registration float not null,
    session_id integer not null,
    song varchar,
    status integer not null,
    ts timestamp not null,
    user_agent varchar not null,
    user_id integer not null
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer not null,
    artist_id varchar(25) not null,
    artist_latitiude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar not null,
    song_id varchar(25) not null,
    title varchar not null,
    duration float not null,
    year integer
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL SORTKEY DISTKEY,
    user_id integer not null,
    level varchar(10) not null,
    song_id integer not null,
    artist_id integer not null,
    session_id integer not null,
    location varchar(25) not null,
    user_agent varchar(255) not null)
    diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY SORTKEY,
    first_name VARCHAR(25) NOT NULL,
    last_name VARCHAR(25) NOT NULL,
    gender VARCHAR(2) NOT NULL,
    level VARCHAR(10) NOT NULL)
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id INTEGER NOT NULL,
    year INTEGER NOT NULL SORTKEY,
    duration INTEGER NOT NULL)
    diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artitst(
    artist_id INTEGER PRIMARY KEY,
    name VARCHAR(25) NOT NULL SORTKEY,
    location VARCHAR(255) NOT NULL,
    latitude float NOT NULL,
    longitude float NOT NULL)
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp SORTKEY primary key,
    hour smallint not null,
    day smallint not null,
    week smallint not null,
    month smallint not null,
    weekday smallint not null)
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_json_path.json'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
""").format(ARN)

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT
    e.ts as start_time,
    e.user_id as user_id,
    e.level as level,
    s.song_id as song_id,
    s.artist_id as artist_id,
    e.session_id as session_id,
    e.location as location,
    e.user_agent as user_agent
    
FROM staging_songs s
JOIN staging_events e ON (e.song = s.title AND e.artist = s.artist_name)
AND e.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT 
    e.user_id as user_id,
    e.first_name as first_name,
    e.last_name as last_name,
    e.gender as gender,
    e.level as level
FROM staging_events e;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT
    s.song_id as song_id,
    s.title as title,
    s.artist_id as artist_id,
    s.year as year,
    s.duration as duration  
FROM staging_songs s

""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT 
    s.artist_id as artist_id,
    s.artist_name as name, 
    s.artist_location as location,
    s.artist_latitude as latitude, 
    s.artist_longitude as longitude
    
FROM staging_songs s
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    weekday)
SELECT 
    ts as start_time,
    EXTRACT(hr from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(yr from start_time) AS year, 
    EXTRACT(weekday from start_time) AS weekday 
FROM (
    SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
    FROM staging_events s     
    )
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
