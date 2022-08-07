# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table songplays(
songplay_id serial primary key, 
start_time timestamp not null, 
user_id int not null, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id int, 
location varchar, 
user_agent varchar)
""")

user_table_create = ("""
create table users(
user_id int primary key,
first_name varchar not null,
last_name varchar,
gender varchar, 
level varchar
)
""")

song_table_create = ("""
create table songs(
song_id varchar primary key,
title varchar not null,
artist_id varchar,
year int,
duration double precision not null
)
""")

artist_table_create = ("""
create table artists(
artist_id varchar primary key,
name varchar not null,
location varchar,
latitude double precision,
longitude double precision
)
""")

time_table_create = ("""
create table time (
start_time timestamp primary key, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
)
""")

# CREATE / DROP TEMP TABLES

temp_users_create = "CREATE TEMPORARY TABLE temp_users (LIKE users)"
temp_users_drop = "DROP TABLE temp_users"

temp_songplays_create = "CREATE TEMPORARY TABLE temp_songplays AS SELECT * FROM songplays WHERE false"
temp_songplays_drop = "DROP TABLE temp_songplays"

# INSERT ONE RECORD

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, "level", song_id, artist_id, session_id, "location", user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, "level")
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) 
DO UPDATE SET 
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    "level" = EXCLUDED."level"
;
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, "year", duration)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) 
DO UPDATE SET 
    "title" = EXCLUDED."title",
    year = EXCLUDED.year,
    duration = EXCLUDED.duration;
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, "name", "location", latitude, longitude)
VALUES (%s,%s,%s,%s,%s)  ON CONFLICT (artist_id) 
DO UPDATE SET 
    "location" = EXCLUDED."location",
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;
""")


time_table_insert = ("""
INSERT INTO "time"
(start_time, "hour", "day", week, "month", "year", weekday)
VALUES(%s,%s,%s,%s,%s, %s, %s) ON CONFLICT DO NOTHING;
""")

# BATCHES QUERIES

temp_users_copy = "COPY temp_users FROM STDIN WITH HEADER CSV"
temp_users_upsert = """
INSERT INTO users
SELECT * FROM temp_users
ON CONFLICT (user_id) 
DO NOTHING
"""

temp_songplays_copy = "COPY temp_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) FROM STDIN WITH HEADER CSV"
temp_songplays_insert = """
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT start_time, user_id, level, song_id, artist_id, session_id, location, user_agent FROM temp_songplays
"""


# FIND SONGS

song_select = ("""
select s.song_id as song_id, s.artist_id as artist_id from songs s inner join artists a on a.artist_id = s.artist_id where s.title = %s and a.name = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]