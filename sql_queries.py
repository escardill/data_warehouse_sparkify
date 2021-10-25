import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(

)
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id bigint not null,
    start_time timestamp,
    user_id integer not null,
    level varchar(10) not null,
    song_id integer not null,
    artist_id integer not null,
    session_id integer not null,
    location varchar(25) not null,
    user_agent varchar(255) not null,
    primary key (songplay_id),
    foreign key (user_id) references users(user_id),
    foreign key (song_id) references songs(songplay_id),
    foreign key (artist_id) references artists(artist_id),
    foreign key (start_time) references time(start_time),
    diststyle key distkey (songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR(25) NOT NULL,
    last_name VARCHAR(25) NOT NULL,
    gender VARCHAR(2) NOT NULL,
    level VARCHAR(10) NOT NULL,
    diststyle all
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    duration INTEGER NOT NULL,
    foreign key (artist_id) references artists(artist_id),
    diststyle all 
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artitst(
    artist_id INTEGER PRIMARY KEY,
    name VARCHAR(25) NOT NULL,
    location VARCHAR(255) NOT NULL,
    latitude float NOT NULL,
    longitude float NOT NULL,
    diststyle all
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp primary key,
    hour smallint not null,
    day smallint not null,
    week smallint not null,
    month smallint not null,
    weekday smallint not null,
    diststyle all
)
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
