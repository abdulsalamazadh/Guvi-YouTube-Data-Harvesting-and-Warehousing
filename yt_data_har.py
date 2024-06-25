# USED LIBRARIES

import os
from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
\
load_dotenv()

# GET CREDENTIALS FROM ENVIRONMENT FILE

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# BUILDING CONNECTION WITH YOstttUTUBE API

def api_connection():

    api_id = YOUTUBE_API_KEY
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey= api_id)
    
    return youtube

youtube = api_connection()


# FUNCTION TO GET CHANNEL DETAILS

def get_channel_info(channel_id):

    request = youtube.channels().list(part = "contentDetails, snippet , statistics", 
                                      id = channel_id
                                      )
    response = request.execute()

    for i in response["items"]:

        channel_data = dict(Channel_name = i["snippet"]["title"],
                    Channel_ID = i["id"], Subscription_Count = i["statistics"]["subscriberCount"],
                    Channel_Views = i["statistics"]["viewCount"], Total_Videos = i["statistics"]["videoCount"], 
                    Channel_Description = i["snippet"]["description"], Playlist_ID = i["contentDetails"]["relatedPlaylists"]["uploads"])
    
    return channel_data

# FUNCTION TO GET VIDEO IDs

def get_video_IDs(channel_id):
    
    Video_IDs = []

    request = youtube.channels().list(
                            part = "contentDetails",
                            id = channel_id
        )
    response = request.execute()
    
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

        request = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_id
        )

        response = request.execute()
        
        for videos in response["items"]:

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
            )
            response = request.execute()

            for comments in response['items']:

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
            pageToken = next_page_token)
        
        response = request.execute()

        for playlist in response['items']:

            Playlist_data = dict(Playlist_Id = playlist['id'],
                                Channel_Name = playlist['snippet']['channelTitle'],
                                Channel_Id = playlist['snippet']['channelId'],
                                PublishedAt = playlist['snippet']['publishedAt'],
                                Video_count = playlist['contentDetails']['itemCount']
            )
            Playlist_Ids.append(Playlist_data)
            
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:

            break
    
    return Playlist_Ids

#FULL CHANNEL INFO

def full_channel_info(Channel_id):

    channel_detail = get_channel_info(Channel_id)
    Playlist_detail = get_playlist_details(Channel_id)
    Video_Id = get_video_IDs(Channel_id)
    Video_Detail = get_video_details(Video_Id)
    Comment_Detail = get_comment_info(Video_Id)
    
    return {"channel_detail": channel_detail,
            "Playlist_detail": Playlist_detail,
            "Video_Detail": Video_Detail,
            "Comment_Detail": Comment_Detail
            }

#CONVERT INTO DATAFRAMES

def dataframes(yt_channel_id):

    channel_full_details = full_channel_info(yt_channel_id)
    print("Data collected successfully....")

    #1 CHANNEL TABLE DF

    df_channel_details = pd.DataFrame([channel_full_details["channel_detail"]])

    #2 PLAYLIST TABLE DF

    df_Playlist = []

    try:
        for i in range(len(channel_full_details["Playlist_detail"])):

            channel_data= channel_full_details["Playlist_detail"][i]
            df = pd.DataFrame([channel_data])
            df_Playlist.append(df)

        df_Playlist_details = pd.concat(df_Playlist, ignore_index=True)

    except Exception as e:
        print("no playlist", e)

    #3 VIDEO TABLE DF

    df_video = []

    for i in range(len(channel_full_details["Video_Detail"])):

        Channel_data= channel_full_details["Video_Detail"][i]
        df = pd.DataFrame([Channel_data])
        df_video.append(df)

    df_video_details = pd.concat(df_video, ignore_index=True)

    #4 COMMENT TABLE DF

    df_comment = []

    for i in range(len(channel_full_details["Comment_Detail"])):
        
        Channel_data= channel_full_details["Comment_Detail"][i]
        df = pd.DataFrame([Channel_data])
        df_comment.append(df)

    df_comment_details = pd.concat(df_comment, ignore_index=True)

    print("collected data to dataframe")

    return df_channel_details, df_Playlist_details, df_video_details, df_comment_details

#TABLE CREATION CHANNELS

def channels_table(df_channel_details):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    try:
        create_query='''create table if not exists Channel(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text, 
                                                            Playlist_Id varchar(80))'''
        my_cursor.execute(create_query)

        print("Channeltable created successfully.")

    except Exception as e:
        print("Error creating table:", e)

    insert_query = '''insert into Channel(Channel_Name,
                                        Channel_Id,
                                        Subscribers,
                                        Views,
                                        Total_Videos,
                                        Channel_Description,
                                        Playlist_Id)
                                        
                                        values(%s, %s, %s, %s, %s, %s, %s)'''
    
    for index, row in df_channel_details.iterrows():

        try:
            my_cursor.execute(insert_query,  (row['Channel_name'],
                    row['Channel_ID'],
                    row['Subscription_Count'],
                    row['Channel_Views'], row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_ID']))
            connect_db.commit()

        except Exception as e:
            connect_db.rollback()

#TABLE CREATION PLAYLISTS

def playlist_table(df_Playlist_details):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    try:
        create_query='''create table if not exists Playlist(Playlist_Id varchar(100) primary key,
                                                            Channel_Name varchar(100),
                                                            Channel_Id varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_count int
                                                            )'''
        my_cursor.execute(create_query)
    
    except Exception as e:
        print("Error creating table:", e)

    insert_query = '''insert into Playlist(Playlist_Id,
                                        Channel_Name,
                                        Channel_Id,
                                        PublishedAt,
                                        Video_count)
                                        
                                        values(%s, %s, %s, %s, %s)'''
        
    for index, row in df_Playlist_details.iterrows():

        try:
            my_cursor.execute(insert_query, (row['Playlist_Id'],
                  row['Channel_Name'],
                  row['Channel_Id'],
                  row['PublishedAt'],
                  row['Video_count']))
            connect_db.commit()
        except Exception as e:
            connect_db.rollback()

#TABLE CREATION VIDEOS

def videos_table(df_video_details):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    
    # try:
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
    
    # except Exception as e:
    #     print("Error creating table:", e)
    for index, row in df_video_details.iterrows():
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
    # for index, row in df_video_details.iterrows():

        # my_cursor.execute(insert_query, (row['Channel_Name'],
        #         row['Channel_Id'],
        #         row['Video_Id'],
        #         row['Video_Name'],
        #         row['Video_Description'],
        #         row['Tags'],
        #         row['PublishedAt'],
        #         row['Views_Count'],
        #         row['Like_Count'],
        #         row['Favorite_Count'],
        #         row['Comment_Count'],
        #         row['Duration'],
        #         row['Thumbnail'],
        #         row['Caption_Status']))
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

def comment_table(df_comment_details):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    my_cursor=connect_db.cursor()

    try:
        create_query='''create table if not exists Comment(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_PublishedAt timestamp
                                                            )'''
        my_cursor.execute(create_query)
    
    except Exception as e:
        print("Error creating table:", e)
    
    
    insert_query = '''insert into Comment(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_PublishedAt)

                                        values(%s, %s, %s, %s, %s)'''
        
    for index, row in df_comment_details.iterrows():

        try:
            my_cursor.execute(insert_query, (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishedAt']))

            # Commit changes to the database
            connect_db.commit()
        except Exception as e:
            connect_db.rollback()

#COLLECTING ALL TABLES

def all_tables(df_channel_details,df_Playlist_details,df_video_details,df_comment_details):

    channels_table(df_channel_details)
    playlist_table(df_Playlist_details)
    videos_table(df_video_details)
    comment_table(df_comment_details)

#INSERT TABLES TO SQL

def insert_tables_to_sql(yt_channel_id):

    channel_df, playlist_df, video_df, comment_df = dataframes(yt_channel_id)
    all_tables(channel_df, playlist_df, video_df, comment_df)

    return "Tables inserted to sql successfully"

def sql_to_dataframe():

    # CONNECTINGG TO POSTGRES
    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    
    my_cursor=connect_db.cursor()
    
    # EXECUTING SQL QUERY
    query = '''select * from channel'''

    my_cursor.execute(query)
    
    # FETCHING ALL ROWS
    result = my_cursor.fetchall()
    
    # GETTING ALL COLUMNS
    columns = [col[0] for col in my_cursor.description]
    
    # CREATING DATAFRAME
    df = pd.DataFrame(result, columns=columns)
    
    # CLOSING CURSOR AND CONNECTION
    my_cursor.close()
    connect_db.close()
    return df

# STREAMLIT APP CREATION

st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

channel_id = st.text_input("Enter the channel ID")

def check_channel_exists(channel_id):

    connect_db=psycopg2.connect(host = POSTGRES_DB_HOST,
                            user = POSTGRES_DB_USER,
                            password = POSTGRES_DB_PASSWORD,
                            database = POSTGRES_DB_NAME,
                            port = POSTGRES_PORT)
    
    cursor = connect_db.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM channel WHERE Channel_Id = %s);", (channel_id,))
    exists = cursor.fetchone()[0]
    connect_db.close()

    return exists

# BUTTON TO COLLECT DATA

if st.button("Collect Data"):

    if channel_id:

        if check_channel_exists(channel_id):

            st.warning("Channel already inserted")

        else:

            insert_tables_to_sql(channel_id)  # Call the function to collect data
            sqltable = sql_to_dataframe()    # Call the function to retrieve data
        
            if sqltable is not None:

                st.success("Data collected and added successfully!")
                channel_table_df = pd.DataFrame(sqltable)
                st.dataframe(channel_table_df)

            else:

                st.error("Failed to retrieve data from SQL")  # Handle case when data retrieval fails
    
    else:

        st.write("Please enter a channel ID")

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