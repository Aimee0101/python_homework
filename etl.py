# %%
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# %%
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

# %%
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

# %%
filepath = r'C:\Users\wh002\Downloads\Python-main\Python-main\data\song_data'
all_files=get_files(filepath)

# Read all JSON files into a list of dataframes
dataframes = [pd.read_json(file, lines=True) for file in all_files]

# Concatenate all dataframes into a single dataframe
song_data = pd.concat(dataframes, ignore_index=True)
print(song_data.head())

# %%
song_data = song_data[['song_id', 'title', 'artist_id', 'year', 'duration']].values
song_data = song_data.tolist()
print(song_data)

# %%
song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

for song_record in song_data:
    cur.execute(song_table_insert, song_record)
    conn.commit()


# %%
song_data = pd.concat(dataframes, ignore_index=True)
print(song_data.head())
artist_data = song_data[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
artist_data = artist_data.tolist()
print(artist_data)

# %%
artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

for song_record in artist_data:
    cur.execute(artist_table_insert, song_record)
    conn.commit()

# %%
filepath = r'C:\Users\wh002\Downloads\Python-main\Python-main\data\log_data'
all_files=get_files(filepath)

# 读取所有 JSON 文件并将它们合并成一个数据框
dataframes = [pd.read_json(file, lines=True) for file in all_files]
log_data = pd.concat(dataframes, ignore_index=True)

# 打印数据框的前几行
print(log_data.head())

# %%
filepath = r'C:\Users\wh002\Downloads\Python-main\Python-main\data\log_data'
all_files=get_files(filepath)

# Read all JSON files into a list of dataframes
dataframes = [pd.read_json(file, lines=True) for file in all_files]

# Concatenate all dataframes into a single dataframe
log_data = pd.concat(dataframes, ignore_index=True)

# Filter records by NextSong action
next_song_data = log_data[log_data['page'] == 'NextSong']

# 将时间戳转换为毫秒并保留两位小数
next_song_data['ts'] = next_song_data['ts'].round(2)

# 然后将转换后的时间戳转换为日期时间格式
next_song_data['ts'] = pd.to_datetime(next_song_data['ts'], unit='ms')

# Extract timestamp, hour, day, week of year, month, year, and weekday
next_song_data['hour'] = next_song_data['ts'].dt.hour
next_song_data['day'] = next_song_data['ts'].dt.day
next_song_data['week_of_year'] = next_song_data['ts'].dt.isocalendar().week
next_song_data['month'] = next_song_data['ts'].dt.month
next_song_data['year'] = next_song_data['ts'].dt.year
next_song_data['weekday'] = next_song_data['ts'].dt.weekday

# Set time_data to a list containing these values in order
time_data = [
    next_song_data['ts'],
    next_song_data['hour'],
    next_song_data['day'],
    next_song_data['week_of_year'],
    next_song_data['month'],
    next_song_data['year'],
    next_song_data['weekday']
]

# Specify labels for these columns
column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']

# Create a dataframe time_df containing the time data
time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

# Display the first few rows of time_df

print(time_df.info())

# %%
time_table_insert =("INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

time_df['hour'] = time_df['hour'].astype(int)
time_df['day'] = time_df['day'].astype(int)
time_df['week_of_year'] = time_df['week_of_year'].astype(int)
time_df['month'] = time_df['month'].astype(int)
time_df['year'] = time_df['year'].astype(int)
time_df['weekday'] = time_df['weekday'].astype(int)
   
    # 为time_data中的每个元组执行INSERT查询

for row in time_df.itertuples(index=False):
    cur.execute(time_table_insert, row)
    conn.commit()

# %%
user_df = log_data[['userId', 'firstName', 'lastName', 'gender', 'level']]
print(user_df.info())
unique_users_df = user_df.drop_duplicates()
total_rows = unique_users_df.shape[0]

print("Total unique users:", total_rows)
print("Total rows in user_df:", len(user_df))
distinct_user_ids_count = user_df['userId'].nunique()
print("Distinct user IDs count:", distinct_user_ids_count)

# %%
user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level")

for i, row in user_df.iterrows():
    if row['userId'] != '':
        cur.execute(user_table_insert, row)
        conn.commit()

# %%
song_select = ("SELECT songs.song_id, artists.artist_id FROM songs \
JOIN artists ON songs.artist_id = artists.artist_id \
WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s")
song_id_list = []
artist_id_list = []

for index, row in log_data.iterrows():
    # Get the values for title, name, and duration from the current row
    title = row['song']
    name = row['artist']
    duration = row['length']

    # Execute the song_select query with the current values of title, name, and duration
    cur.execute(song_select, (title, name, duration))
    # Fetch the result
    result = cur.fetchone()

    if result:
        song_id, artist_id = result
        song_id_list.append(song_id)
        artist_id_list.append(artist_id)
    else:
        # If song_select query doesn't return any result, set song_id and artist_id to None
        song_id_list.append(None)
        artist_id_list.append(None)

log_data['song_id'] = song_id_list
log_data['artist_id'] = artist_id_list
   
songplay_data = log_data[['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']]

print(songplay_data.head())

songplay_table_insert = ("INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

for index, row in songplay_data.iterrows():
    if row['userId'] == '':
        continue
    # Get the values from the current row
    start_time = pd.to_datetime(row['ts'], unit='ms')
    user_id = row['userId']
    level = row['level']
    song_id = row['song_id']
    artist_id = row['artist_id']
    session_id = row['sessionId']
    location = row['location']
    user_agent = row['userAgent']
    
    # Execute the songplay_table_insert query with the values from the current row
    cur.execute(songplay_table_insert, (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent))
    
    # Commit the transaction
    conn.commit()

# %%
# Close Cursor
try:
    cur.close()
except:
    pass

# Close Connection
try:
    conn.close()
    print("Connection to Sparkify database closed successfully")
except:
    print("Error occurred while closing the connection")


