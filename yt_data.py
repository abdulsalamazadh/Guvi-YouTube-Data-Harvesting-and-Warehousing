import os
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

from dotenv import load_dotenv

load_dotenv()

# GET CREDENTIALS FROM ENVIRONMENT FILE

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_DB_COLLECTION = os.getenv("MONGO_DB_COLLECTION")

# BUILDING CONNECTION WITH YOUTUBE API

def api_connection():
    api_id = YOUTUBE_API_KEY
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey= api_id)
    return youtube

youtube = api_connection()


# FUNCTION TO GET CHANNEL DETAILS

def get_channel_info(channel_id):
    request = youtube.channels().list(part = "contentDetails, snippet , statistics", id = channel_id).execute()

    for i in request["items"]:
        channel_data = dict(Channel_name = i["snippet"]["title"],
                    Channel_ID = i["id"], Subscription_Count = i["statistics"]["subscriberCount"],
                    Channel_Views = i["statistics"]["viewCount"], Total_Videos = i["statistics"]["videoCount"], 
                    Channel_Description = i["snippet"]["description"], Playlist_ID = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return channel_data

# FUNCTION TO GET VIDEO IDs

def get_video_IDs(channel_id):
    
    Video_IDs = []
    response = youtube.channels().list(
                            part = "contentDetails",
                            id = channel_id
        ).execute()
    
    Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response = youtube.playlistItems().list(part = 'snippet', playlistId = Playlist_ID, maxResults = 50, pageToken = next_page_token).execute()

        for i in range(len(response['items'])):
            Video_IDs.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return Video_IDs

# FUNCTION TO GET VIDEO DETAILS 

def get_video_details(video_Id):
    Video_details = []

    for video_id in video_Id:
        request = youtube.videos().list( part = "snippet,ContentDetails,statistics", id = video_id).execute()
        
        for videos in request["items"]:
            Video_data = dict(Channel_Name = videos['snippet']['channelTitle'],
                    Channel_Id = videos['snippet']['channelId'],
                    Video_Id = videos['id'], Video_Name = videos['snippet']['title'], Video_Description = videos['snippet'].get('description'),
                    Tags = videos['snippet'].get('tags'), PublishedAt = videos['snippet']['publishedAt'], 
                    Views_Count = videos['statistics'].get('viewCount'),
                    Like_Count = videos['statistics'].get('likeCount'), 
                    Favorite_Count = videos['statistics'].get('favoriteCount'), 
                    Comment_Count = videos['statistics'].get('commentCount'), Duration = videos['contentDetails']['duration'], 
                    Thumbnail = videos['snippet']['thumbnails']['default']['url'], Caption_Status = videos['contentDetails']['caption'])
            Video_details.append(Video_data)
    return Video_details

# FUNCTION TO GET COMMENT DETAILS

def get_comment_info(Video_Ids):

    Comment_details = []

    try:
        for video_id in Video_Ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            ).execute()

            for comments in request['items']:
                comments_data = dict(Comment_Id = comments['snippet']['topLevelComment']['id'],
                                    Video_Id = comments['snippet']['topLevelComment']['snippet']['videoId'],
                                    Comment_Text = comments['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_Author = comments['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Comment_PublishedAt = comments['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment_details.append(comments_data)
                
    except:
        pass
    return Comment_details

# FUNCTION TO GET PLAYLIST ID

def get_playlist_details(Channel_ID):

    next_page_token = None

    Playlist_Ids = []

    while True:

        request = youtube.playlists().list(
            part = 'snippet, contentDetails',
            channelId = Channel_ID,
            maxResults = 50,
            pageToken = next_page_token).execute()

        for playlist in request['items']:
            Playlist_data = dict(Playlist_Id = playlist['id'],
                                Channel_Name = playlist['snippet']['channelTitle'],
                                Channel_Id = playlist['snippet']['channelId'],
                                PublishedAt = playlist['snippet']['publishedAt'],
                                Video_count = playlist['contentDetails']['itemCount']
            )
            Playlist_Ids.append(Playlist_data)
            
        next_page_token = request.get('nextPageToken')
        if next_page_token is None:
            break
    
    return Playlist_Ids

#CONNECT WITH DB USING MANGO DB

client = pymongo.MongoClient(MONGO_DB_URL)

database = client[MONGO_DB_NAME]

def channel_details(Channel_id):
    channel_detail = get_channel_info(Channel_id)
    Playlist_detail = get_playlist_details(Channel_id)
    Video_Id = get_video_IDs(Channel_id)
    Video_Detail = get_video_details(Video_Id)
    Comment_Detail = get_comment_info(Video_Id)

    collection = database[MONGO_DB_COLLECTION]
    collection.insert_one({"Channel":channel_detail,"Playlist":Playlist_detail,
                      "Videos":Video_Detail,"Comment":Comment_Detail})
    
    return "Uploaded successfully"

#TABLE CREATION CHANNELS

def channels_table(s_channel_name):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    create_query='''create table if not exists Channel(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text, 
                                                        Playlist_Id varchar(80))'''
    my_cursor.execute(create_query)
    connect_db.commit()

    Channel_detail = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for ch_data in collection.find({"Channel.Channel_name": s_channel_name}, {"_id": 0}):
        Channel_detail.append(ch_data["Channel"])

    df_channel_detail = pd.DataFrame(Channel_detail)
    
    for index, row in df_channel_detail.iterrows():
        insert_query = '''insert into Channel(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views, Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s, %s, %s, %s, %s, %s, %s)'''
        values = (row['Channel_name'],
                row['Channel_ID'],
                row['Subscription_Count'],
                row['Channel_Views'], row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_ID'])
        
        try:
            my_cursor.execute(insert_query, values)
            connect_db.commit()
        except:
            statement = f"The channel name {s_channel_name} is already exists"
            return statement

#TABLE CREATION PLAYLISTS

def playlist_table(s_channel_name):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    create_query='''create table if not exists Playlist(Playlist_Id varchar(100) primary key,
                                                        Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_count int
                                                        )'''
    my_cursor.execute(create_query)
    connect_db.commit()

    Channel_detail = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for ch_data in collection.find({"Channel.Channel_name": s_channel_name}, {"_id": 0}):
        Channel_detail.append(ch_data["Playlist"])

    df_channel_detail = pd.DataFrame(Channel_detail[0])

    for index, row in df_channel_detail.iterrows():
        insert_query = '''insert into Playlist(Playlist_Id,
                                            Channel_Name,
                                            Channel_Id,
                                            PublishedAt,
                                            Video_count)
                                            
                                            values(%s, %s, %s, %s, %s)'''
        values = (row['Playlist_Id'],
                  row['Channel_Name'],
                  row['Channel_Id'],
                  row['PublishedAt'],
                  row['Video_count'])
        
        my_cursor.execute(insert_query, values)
        connect_db.commit()

#TABLE CREATION VIDEOS

def videos_table(s_channel_name):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    create_query='''create table if not exists Videos(Channel_Name varchar(100), 
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(100) primary key, 
                                                    Video_Name varchar(150), 
                                                    Video_Description text,
                                                    Tags text, 
                                                    PublishedAt timestamp, 
                                                    Views_Count bigint,
                                                    Like_Count bigint, 
                                                    Favorite_Count int, 
                                                    Comment_Count int, 
                                                    Duration interval, 
                                                    Thumbnail varchar(200), 
                                                    Caption_Status varchar(50)
                                                        )'''
    my_cursor.execute(create_query)
    connect_db.commit()

    Channel_detail = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for ch_data in collection.find({"Channel.Channel_name": s_channel_name}, {"_id": 0}):
        Channel_detail.append(ch_data["Videos"])

    df_channel_detail = pd.DataFrame(Channel_detail[0])

    for index, row in df_channel_detail.iterrows():
        insert_query = '''insert into Videos(Channel_Name, 
                                                Channel_Id,
                                                Video_Id,
                                                Video_Name, 
                                                Video_Description,
                                                Tags, 
                                                PublishedAt, 
                                                Views_Count,
                                                Like_Count, 
                                                Favorite_Count, 
                                                Comment_Count, 
                                                Duration, 
                                                Thumbnail, 
                                                Caption_Status)
                                                
                                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Video_Name'],
                    row['Video_Description'],
                    row['Tags'],
                    row['PublishedAt'],
                    row['Views_Count'],
                    row['Like_Count'],
                    row['Favorite_Count'],
                    row['Comment_Count'],
                    row['Duration'],
                    row['Thumbnail'],
                    row['Caption_Status'])
        
        my_cursor.execute(insert_query, values)
        connect_db.commit()

#TABLE CREATION COMMENTS

def comment_table(s_channel_name):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    create_query='''create table if not exists Comment(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_PublishedAt timestamp
                                                        )'''
    my_cursor.execute(create_query)
    connect_db.commit()

    Channel_detail = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for ch_data in collection.find({"Channel.Channel_name": s_channel_name}, {"_id": 0}):
        Channel_detail.append(ch_data["Comment"])

    df_channel_detail = pd.DataFrame(Channel_detail[0])
    for index, row in df_channel_detail.iterrows():
        insert_query = '''insert into Comment(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_PublishedAt)

                                            values(%s, %s, %s, %s, %s)'''
        
        values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishedAt'])

        my_cursor.execute(insert_query, values)
        connect_db.commit()

#Single function to call all the Table Creation functions

def tables(single_channel):
    
    statement = channels_table(single_channel)
    if statement:
        st.write(statement)
    else:
        playlist_table(single_channel)
        videos_table(single_channel)
        comment_table(single_channel)

    return "All Tables and Values Loaded Successfully to SQL Database"

# Streamlit Channel information Dataframe

def view_channel_table():

    Channel_list = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for ch_data in collection.find({}, {"_id": 0, "Channel": 1}):
        Channel_list.append(ch_data["Channel"])

    df = st.dataframe(Channel_list)

    return df

# Streamlit Playlist information Dataframe

def view_playlist_table():
        
    Playlist_list = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for pl_data in collection.find({}, {"_id": 0, "Playlist": 1}):
        for i in range(len(pl_data['Playlist'])):
            Playlist_list.append(pl_data["Playlist"][i])
            
    df1 = st.dataframe(Playlist_list)

    return df1


# Streamlit Videos information Dataframe

def view_videos_table():

    Videos_list = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for vi_data in collection.find({}, {"_id": 0, "Videos": 1}):
        for i in range(len(vi_data['Videos'])):
            Videos_list.append(vi_data['Videos'][i])

    df2 = st.dataframe(Videos_list)

    return df2

# Streamlit Comment information Dataframe

def view_comment_table():

    Comment_list = []

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]

    for cm_data in collection.find({}, {"_id": 0, "Comment": 1}):
        for i in range(len(cm_data['Comment'])):
            Comment_list.append(cm_data["Comment"][i])

    df3 = st.dataframe(Comment_list)
    
    return df3

# Streamlit Application creation

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.title(":blue[Abdul Salam Azadh Z]")
    st.header("Skills Take Away")
    st.caption("Python Programming")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and Store"):
    Channel_ids = []
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_DB_COLLECTION]
    for ch_data in collection.find({},{"_id":0, "Channel":1}):
        Channel_ids.append(ch_data['Channel']['Channel_ID'])

    if channel_id in Channel_ids:
        st.success("Given Channel ID datas are already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

all_channels = []

db = client[MONGO_DB_NAME]
collection = db[MONGO_DB_COLLECTION]

for ch_data in collection.find({}, {"_id": 0, "Channel": 1}):
    all_channels.append(ch_data["Channel"]["Channel_name"])

Channel = st.selectbox("Select Channel", all_channels)

if st.button("Migrate to SQL"):
    tables = tables(Channel)
    st.success(tables)

show_table = st.radio("Select the Table:", ("Channel", "Playlist", "Comment", "Video"))

if show_table == "Channel":
    view_channel_table()

elif show_table == "Playlist":
    view_playlist_table()

elif show_table == "Video":
    view_videos_table()

elif show_table == "Comment":
    view_comment_table()



# SQL Connection and SQL Query Output need to displayed

connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
my_cursor=connect_db.cursor()

questions = st.selectbox("Select the Question", ("1. What are the names of all the videos and their corresponding channels?", 
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?", 
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if questions == "1. What are the names of all the videos and their corresponding channels?":
    query_1 = '''select video_name as videos, channel_name as channelname from videos'''
    my_cursor.execute(query_1)
    connect_db.commit()

    data_1 = my_cursor.fetchall()

    df_1 = pd.DataFrame(data_1, columns=["Videos Title", "Channel Name"])

    st.write(df_1)

elif questions == "2. Which channels have the most number of videos, and how many videos do they have?":
    query_2 = '''SELECT channel_name, COUNT(*) AS video_count FROM videos GROUP BY channel_name ORDER BY video_count DESC'''
    my_cursor.execute(query_2)
    connect_db.commit()

    data_2 = my_cursor.fetchall()

    df_2 = pd.DataFrame(data_2, columns=["Channel Name", "No. of Videos"])

    st.write(df_2)

elif questions == "3. What are the top 10 most viewed videos and their respective channels?":
    query_3 = '''select channel_name, video_name, views_count from videos order by views_count desc limit 10'''
    my_cursor.execute(query_3)
    connect_db.commit()

    data_3 = my_cursor.fetchall()

    df_3 = pd.DataFrame(data_3, columns=["Channel Name", "Videos_Title", "No. of Views"])

    st.write(df_3)

elif questions == "4. How many comments were made on each video, and what are their corresponding video names?":
    query_4 = '''select video_name, comment_count from videos'''
    my_cursor.execute(query_4)
    connect_db.commit()

    data_4 = my_cursor.fetchall()

    df_4 = pd.DataFrame(data_4, columns=["Videos_Title", "No. of Comments"])

    st.write(df_4)


elif questions == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query_5 = '''select video_name, channel_name, like_count from videos where like_count is not null order by like_count desc'''
    my_cursor.execute(query_5)
    connect_db.commit()

    data_5 = my_cursor.fetchall()

    df_5 = pd.DataFrame(data_5, columns=["Video Name", "Channel Name", "No. of Likes"])

    st.write(df_5)

elif questions == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query_6 = '''select video_name, like_count, favorite_count from videos where like_count is not null order by like_count desc'''
    my_cursor.execute(query_6)
    connect_db.commit()

    data_6 = my_cursor.fetchall()

    df_6 = pd.DataFrame(data_6, columns=["Videos_Title", "No. of Likes", "No. of Favourites"])

    st.write(df_6)

elif questions == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query_7 = '''select channel_name, views from channel order by views desc'''
    my_cursor.execute(query_7)
    connect_db.commit()

    data_7 = my_cursor.fetchall()

    df_7 = pd.DataFrame(data_7, columns=["Channel Name", "Total no. of views"])

    st.write(df_7)

elif questions == "8. What are the names of all the channels that have published videos in the year 2022?":
    query_8 = '''select distinct c.channel_name from channel c join videos v on c.channel_id = v.channel_id where extract(year from v.publishedat) = 2022'''
    my_cursor.execute(query_8)
    connect_db.commit()

    data_8 = my_cursor.fetchall()

    df_8 = pd.DataFrame(data_8, columns=["Channel Name"])

    st.write(df_8)

elif questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query_9 = '''select distinct c.channel_name, avg(v.duration) as average_duration from channel c join videos v on c.channel_id = v.channel_id group by c.channel_name'''
    my_cursor.execute(query_9)
    connect_db.commit()

    data_9 = my_cursor.fetchall()

    df_9 = pd.DataFrame(data_9, columns=["Channel Name", "Average Duration"])

    Ques_9=[]
    
    for index,row in df_9.iterrows():
        channel_title=row["Channel Name"]
        average_duration=row["Average Duration"]
        average_duration_str=str(average_duration)
        Ques_9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df9 = pd.DataFrame(Ques_9)

    st.write(df_9)

elif questions == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query_10 = '''select video_name, channel_name, comment_count from videos where comment_count is not null order by comment_count desc'''
    my_cursor.execute(query_10)
    connect_db.commit()

    data_10 = my_cursor.fetchall()

    df_10 = pd.DataFrame(data_10, columns=["Video Name", "Channel Name", "No. of Comments"])

    st.write(df_10)