# Project: Data Modeling with Postgres

In this project, we will process an ETL from the logs and songs JSON files to create a Snowflake Schema on song plays, as currently, they don't have an easy way to query the data. The purpose is to create an optimized schema for analytical queries on the dataset to get business insights with song play analysis. 

# How to run

Open a terminal and run the follow scripts:

``` bash
python create_tables.py
python etl.py
```

# ETL Process

On the data/ folder, we have two types of data:
- song_data, that is JSON files containing the description of songs and artists.
- log_data, JSON files that contains information about the user actions on the site.
The scripts create tables, parse the data and insert them on the database to a analytical database.

# Files in the project

- **sql_queries.py** contains the sql queries used on the project.
- **create_tables.py** contains a script that drops and recreates tables every time that runs. Have to run before the etl.py.
- **etl.py** contains the scripts that reads and processes files from song_data and log_data to import to the database.
- **etl.ipynb** contains the exploratory work of the song_data and log_data files.
- **test.ipynb** show some queries to see the state of the database.

# Database Design

We choose the Snowflake Schema because of the relation between our dimensions songs and artists and because it is opmized for queries.

## Fact Table
- songplays -> records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
- users -> users in the app 
    - user_id, first_name, last_name, gender, level
- songs -> songs in music database 
    - song_id, title, artist_id, year, duration
- artists -> artists in music database
    - artist_id, name, location, latitude, longitude
- time -> timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

# Example queries
How many time a song was listen on a week?
``` sql
%sql select count(*), week from songplays s inner join time t on s.start_time = t.start_time where song_id = 'SOZCTXZ12AB0182364' and week = 47 group by t.week
```