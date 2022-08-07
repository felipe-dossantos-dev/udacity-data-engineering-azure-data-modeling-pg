import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np
from datetime import datetime
from io import StringIO


def process_song_file(cur, filepath):
    """
    - Open a song file
    - Transform the data
    - Insert song and artist records
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Open a log file
    - Filter by NextSong
    - Convert timestamp column to datetime
    - Insert time data records
    - Map the users data
    - Insert the users record
    - Get song_id and artist_id from each songplay
    - Map the songplay data
    - Insert the songplay record
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_data = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    upsert_bulk_users(cur, user_df)

    # insert songplay records
    songplays_data = []
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        filter_data = (row.song, row.artist, row.length)
        cur.execute(song_select, filter_data)
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        dt_object = datetime.fromtimestamp(row.ts / 1000.0)
        songplay_data = (dt_object, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        songplays_data.append(songplay_data)
             
    songplay_df = pd.DataFrame(songplays_data, columns =['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])
    insert_bulk_songplays(cur, songplay_df)


def process_data(cur, conn, filepath, func):
    """
    - Get files from directory
    - For each file
        - Execute the func passed
        - Commit the inserts
        - Log the results
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def dt_to_csv_buffer(df):
    """ Transform a dataframe to csv buffer """
    buffer = StringIO()
    df.to_csv(buffer, header=True, index=False)
    buffer.seek(0)
    return buffer
    
def upsert_bulk_users(cur, df):
    """
    - drop duplicates
    - create a csv on memory from df
    - create a temp table
    - add all data to temp table
    - upsert data on table
    - drop temp table
    """
    df = df.drop_duplicates(subset=['userId'], keep='last')
    buffer = dt_to_csv_buffer(df)
    cur.execute(temp_users_create)
    cur.copy_expert(temp_users_copy, buffer)
    cur.execute(temp_users_upsert)
    cur.execute(temp_users_drop)
    
def insert_bulk_songplays(cur, df):
    """
    - create a csv on memory from df
    - create a temp table
    - add all data to temp table
    - insert data on table
    - drop temp table
    """
    buffer = dt_to_csv_buffer(df)    
    cur.execute(temp_songplays_create)
    cur.copy_expert(temp_songplays_copy, buffer)    
    cur.execute(temp_songplays_insert)
    cur.execute(temp_songplays_drop)
    
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()