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
    auth varchar,
    first_name varchar,
    gender varchar(1),
    item_in_session integer,
    last_name varchar,
    length float,
    level varchar(5),
    location varchar,
    method varchar(5),
    page varchar,
    registration float,
    session_id integer,
    song varchar,
    status integer,
    ts bigint,
    user_agent varchar,
    user_id integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer ,
    artist_id varchar(25) ,
    artist_latitiude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar ,
    song_id varchar(25) ,
    title varchar ,
    duration float ,
    year integer
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp  NOT NULL SORTKEY DISTKEY references time (start_time),
    user_id integer NOT NULL references users (user_id),
    level varchar(10) ,
    song_id integer NOT NULL references songs (song_id),
    artist_id integer NOT NULL references artists (artist_id),
    session_id integer NOT NULL,
    location varchar(25) ,
    user_agent varchar(255) )
    diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id integer PRIMARY KEY SORTKEY,
    first_name varchar(25) ,
    last_name varchar(25) ,
    gender varchar(2) ,
    level varchar(10) )
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id integer PRIMARY KEY,
    title varchar(255) ,
    artist_id integer ,
    year integer  SORTKEY,
    duration integer )
    diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id integer PRIMARY KEY,
    name varchar(25)  NOT NULL SORTKEY,
    location varchar(255) ,
    latitude float ,
    longitude float )
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp PRIMARY KEY SORTKEY,
    hour smallint NOT NULL,
    day smallint NOT NULL,
    week smallint NOT NULL,
    month smallint NOT NULL,
    weekday smallint NOT NULL)
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} CREDENTIALS 'aws_iam_role={}' region 'us-west-2' json {};
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""    
COPY staging_songs FROM
    {} CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto' truncatecolumns;
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))




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
WHERE e.page = 'NextSong'
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
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
