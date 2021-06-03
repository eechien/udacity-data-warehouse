import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS sparkify_user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_event (
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
        registration bigint,
        sessionId integer,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId varchar
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_song (
        num_songs integer,
        artist_id varchar NOT NULL,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year integer
    )    
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id integer IDENTITY(0,1),
        start_time timestamp NOT NULL,
        user_id integer NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        session_id integer NOT NULL,
        location varchar NOT NULL,
        user_agent varchar NOT NULL,
        primary key (songplay_id)
    ) DISTSTYLE EVEN;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify_user (
        user_id integer NOT NULL,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar NOT NULL,
        primary key (user_id)
    ) DISTSTYLE EVEN;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id varchar NOT NULL DISTKEY,
        title varchar NOT NULL,
        artist_id varchar,
        year integer NOT NULL,
        duration decimal NOT NULL,
        primary key (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id varchar NOT NULL DISTKEY,
        name varchar NOT NULL,
        location varchar,
        latitude float,
        longitude float,
        primary key (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL DISTKEY,
        hour integer NOT NULL,
        day integer NOT NULL,
        week integer NOT NULL,
        month integer NOT NULL,
        year integer NOT NULL,
        weekday integer NOT NULL,
        primary key (start_time)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_event from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    json {};
""")

staging_songs_copy = ("""
    copy staging_song from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    compupdate off region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    )
    SELECT
        timestamp 'epoch' + e.ts * interval '1 second',
        cast(e.userId as integer),
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_event e
    INNER JOIN staging_song s ON (e.song=s.title AND e.artist=s.artist_name AND e.length=s.duration)
    WHERE e.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO sparkify_user (
        user_id, first_name, last_name, gender, level
    )
    SELECT cast(userId as integer), firstName, lastName, gender, level
    FROM staging_event
    WHERE page='NextSong';
""")

song_table_insert = ("""
    INSERT INTO song (
        song_id, title, artist_id, year, duration
    )
    SELECT song_id, title, artist_id, year, duration
    FROM staging_song;
""")

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id, name, location, latitude, longitude
    )
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_song;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, hour, day, week, month, year, weekday
    )
    SELECT
        start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dow from start_time)
    FROM songplay;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
