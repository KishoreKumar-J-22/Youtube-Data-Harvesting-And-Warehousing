from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


# Define the API key and the API key connection'

def API_connect():
    api_ID = 'AIzaSyDqGtKMPORVgTGOmeoXFfUJquNflcSPzPk'
    api_service_name = 'youtube'
    api_version = 'v3'

    youtube = build(api_service_name,api_version,developerKey=api_ID)

    return youtube
youtube = API_connect()


# Get Channels Information

def get_channel_info(channel_id):
      request = youtube.channels().list(
                  part = "snippet,ContentDetails,statistics",
                  id = channel_id

      )
      response = request.execute()

      for i in  response['items']:
            data = dict(channel_name = i['snippet']['title'],
            channel_id = i['id'],
            subscribers = i['statistics']['subscriberCount'],
            total_videos = i['statistics']['videoCount'],
            total_view_count = i['statistics']['viewCount'],
            channel_description = i['snippet']['description'],
            playlist_ID = i['contentDetails']['relatedPlaylists']['uploads'] 
                  )
      return data


# To ge videos IDS

def get_Video_IDs(channel_id):

  video_IDs = []
  response = youtube.channels().list(id = channel_id,
                                    part = 'contentDetails').execute()
  playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token = None

  while True:

      response_1 = youtube.playlistItems().list(
                                              part = 'snippet',
                                              playlistId = playlist_ID,
                                              maxResults = 50,
                                              pageToken = next_page_token).execute()
      for i in range(len(response_1['items'])):
        video_IDs.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token = response_1.get('nextPageToken')

      if next_page_token is None:
        break
  return video_IDs



# Get video Information

def get_video_info(Video_IDs):

    Video_data = []
    for video_id in Video_IDs:
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = video_id

        )
        response = request.execute()
        for item in response['items']:
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        Title = item['snippet']['title'],
                        Tags =item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_status = item['contentDetails']['caption'],)
        Video_data.append(data)
    return Video_data


# to get comment information

def Get_comment_info(video_ids):
    Comment_Data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId= video_id,
                maxResults = 50
            )
            response=request.execute()
            
            for item in response['items']:
                data = dict(comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_published_date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                Comment_Data.append(data)
    except:
        pass
    return Comment_Data


# get playlist info

def get_playlist_details(channel_ID):

    Next_page_token = None
    All_data = []
    while True:
        request =youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_ID,
            maxResults = 50,
            pageToken = Next_page_token
            
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_name = item['snippet']['channelTitle'],
                        Published_At = item['snippet']['publishedAt'],
                        video_count = item['contentDetails']['itemCount'])
            All_data.append(data)
        Next_page_token = response.get('nextPageToken')
        if Next_page_token is None:
            break
    return All_data


# Upload to MongoDB

client = pymongo.MongoClient("mongodb+srv://Kishore:mongodb123@cluster-kishore.kznavjg.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_Data"]


# to insert all the channel details in MongoDB with one function

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_id = get_Video_IDs(channel_id)
    vi_details = get_video_info(vi_id)
    comment_info = Get_comment_info(vi_id)

    collection_1 = db["Channel_details"]
    collection_1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                             "video_information":vi_details,"comment_information":comment_info})
    return "upload completed successfully"

   



# Channel Table creation Channels

# connection to SQL
def channels_table():
    my_database = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "admin123",
                                database = "Youtube_Data",
                                port = "5432")
    cursor = my_database.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    my_database.commit()

# table creation query for channel table

    try:
        create_query = '''create table if not exists channels(channel_name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subscribers bigint,
                                                            total_view_count bigint,
                                                            total_videos int,
                                                            channel_description text,
                                                            playlist_ID varchar(80))'''
        cursor.execute(create_query)
        my_database.commit()

    except:
        print("Channel table already created")

# converting data into DataFrame

    channel_list = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
    df = pd.DataFrame(channel_list)

# Inserting the channel column in SQL

    for index,row in df.iterrows():
        insert_query = '''insert into channels(channel_name,
                                            channel_id,
                                                subscribers,
                                                total_videos,
                                                total_view_count,
                                                channel_description,
                                                playlist_ID)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                row['channel_id'],
                row['subscribers'],
                row['total_videos'],
                row['total_view_count'],
                row['channel_description'],
                row['playlist_ID'])
        
        try:
            cursor.execute(insert_query,values)
            my_database.commit()

        except:
            print("Channel values are already inserted")


# Playlists table creation
            
# connection to SQL

def playlist_table():

    my_database = psycopg2.connect(host = "localhost",
                                    user = "postgres",
                                    password = "admin123",
                                    database = "Youtube_Data",
                                    port = "5432")
    cursor = my_database.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    my_database.commit()

# table creation query for Playlist table
   
    create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(80),
                                                        Channel_Id varchar(100),
                                                        Channel_name varchar(100),
                                                        Published_At timestamp,
                                                        video_count int
                                                        )'''


    cursor.execute(create_query)
    my_database.commit()

# converting data into DataFrame

    playlist_list = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for pl_data in collection_1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(playlist_list)

# Inserting the playlist column in SQL

    for index,row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                            Title,
                                                Channel_Id,
                                                Channel_name,
                                                Published_At,
                                                video_count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        
        
        

        values = (row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_name'],
                row['Published_At'],
                row['video_count'],
        )

        cursor.execute(insert_query,values)
        my_database.commit()



# Videos table creation

def videos_table():

        my_database = psycopg2.connect(host = "localhost",
                                        user = "postgres",
                                        password = "admin123",
                                        database = "Youtube_Data",
                                        port = "5432")
        cursor = my_database.cursor()

        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        my_database.commit()

        #try:
        create_query = '''create table if not exists videos(channel_name varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(30) primary key,
                                                        Title varchar(200),
                                                        Tags text,
                                                        Thumbnail varchar(255),
                                                        Description text,
                                                        Published_date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_count int,
                                                        Definition varchar(10),
                                                        Caption_status varchar(100)
                                                        )'''



        cursor.execute(create_query)
        my_database.commit()



        video_list = []
        db = client["Youtube_Data"]
        collection_1 = db["Channel_details"]
        for vi_data in collection_1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        video_list.append(vi_data["video_information"][i])
        df2 = pd.DataFrame(video_list)


        for index,row in df2.iterrows():
                insert_query = '''insert into videos(channel_name,
                                                        channel_id,
                                                        video_id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_count,
                                                        Definition,
                                                        Caption_status)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                


                values = (row['channel_name'],
                        row['channel_id'],
                        row['video_id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_count'],
                        row['Definition'],
                        row['Caption_status']
                )

                cursor.execute(insert_query,values)
                my_database.commit()
        #except:
        #print("Channel table already created")
                

# Comments table creation
                
def comments_table():

    my_database = psycopg2.connect(host = "localhost",
                                    user = "postgres",
                                    password = "admin123",
                                    database = "Youtube_Data",
                                    port = "5432")
    cursor = my_database.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    my_database.commit()

    #try:
    create_query = '''create table if not exists comments(comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_text text,
                                                            Comment_author varchar(150),
                                                            Comment_published_date timestamp
                                                            )'''

    cursor.execute(create_query)
    my_database.commit()


    comment_list = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for com_data in collection_1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(comment_list)


    for index,row in df3.iterrows():
        insert_query = '''insert into comments(comment_Id,
                                                    Video_Id,
                                                    Comment_text,
                                                    Comment_author,
                                                    Comment_published_date
                                                    )
                                                
                                                values(%s,%s,%s,%s,%s)'''
        


        values = (  row['comment_Id'],
                    row['Video_Id'],
                    row['Comment_text'],
                    row['Comment_author'],
                    row['Comment_published_date']
                    )

        cursor.execute(insert_query,values)
        my_database.commit()


# 
        
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"


#

def show_channels_table():

    channel_list = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
    df = st.dataframe(channel_list)

    return df


#

def show_playlists_table():
    playlist_list = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for pl_data in collection_1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(playlist_list)

    return df1


#

def show_video_table():    
        video_list = []
        db = client["Youtube_Data"]
        collection_1 = db["Channel_details"]
        for vi_data in collection_1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        video_list.append(vi_data["video_information"][i])
        df2 = st.dataframe(video_list)

        return df2


#

def show_comments_table():

    comment_list = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for com_data in collection_1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(comment_list)

    return df3


# Streamlit part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")


channel_id = st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids = []
    db = client["Youtube_Data"]
    collection_1 = db["Channel_details"]
    for channel_data in collection_1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(channel_data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel details of the given channel id is already exists")

    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table = tables()
    st.success(Table)

show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()

elif show_table == "PLAYLISTS":
    show_playlists_table()

elif show_table == "VIDEOS":
    show_video_table()

elif show_table == "COMMENTS":
    show_comments_table()


# Question Part

# SQL connection

my_database = psycopg2.connect(host = "localhost",
                                    user = "postgres",
                                    password = "admin123",
                                    database = "Youtube_Data",
                                    port = "5432")
cursor = my_database.cursor()

question = st.selectbox("Select your question",("1. All the videos and channel name",
                                                "2. Channel with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with highest number of comments"))

if question == "1. All the videos and channel name":

    query_1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query_1)
    my_database.commit()
    table_1 = cursor.fetchall()
    df = pd.DataFrame(table_1,columns = ["video title","channel name"])
    st.write(df)

elif question == "2. Channel with most number of videos":

    query_2 = '''select channel_name as channelname,total_videos as no_videos from channels
                    order by total_videos desc'''
    cursor.execute(query_2)
    my_database.commit()
    table_2 = cursor.fetchall()
    df1 = pd.DataFrame(table_2,columns = ["channel name","No of videos"])
    st.write(df1)

elif question == "3. 10 most viewed videos":

    query_3 = '''select views as views, channel_name as channelname, title as videotitle from videos
                    where views is not null order by views desc limit 10'''
    cursor.execute(query_3)
    my_database.commit()
    table_3 = cursor.fetchall()
    df2 = pd.DataFrame(table_3,columns = ["views","channel name","videotitle"])
    st.write(df2)

elif question == "4. Comments in each videos":

    query_4 = '''select comments as no_comments, title as videotitle from videos where comments is not null'''
    cursor.execute(query_4)
    my_database.commit()
    table_4 = cursor.fetchall()
    df3 = pd.DataFrame(table_4,columns = ["No of comments","videotitle"])
    st.write(df3)

elif question == "5. Videos with highest likes":

    query_5 = '''select title as videotitle, channel_name as channelname, likes as likecount
                    from videos where likes is not null order by likes desc'''
    cursor.execute(query_5)
    my_database.commit()
    table_5 = cursor.fetchall()
    df4 = pd.DataFrame(table_5,columns = ["videotitle","channelname","likecount"])
    st.write(df4)

elif question == "6. Likes of all videos":
    query_6 = '''select likes as likecount, title as videotitle from videos '''
    cursor.execute(query_6)
    my_database.commit()
    table_6 = cursor.fetchall()
    df5 = pd.DataFrame(table_6,columns = ["likecount","videotitle"])
    st.write(df5)

elif question == "7. Views of each channel":
    query_7 = '''select channel_name as channelname ,Views as totalviews from  videos '''
    cursor.execute(query_7)
    my_database.commit()
    table_7 = cursor.fetchall()
    df6 = pd.DataFrame(table_7,columns = ["channelname","total views"])
    st.write(df6)

elif question == "8. Videos published in the year of 2022":
    query_8 = '''select title as videotitle, Published_date as videorelease, channel_name as channelname
                from videos where extract (year from Published_date)=2022'''
    cursor.execute(query_8)
    my_database.commit()
    table_8 = cursor.fetchall()
    df7 = pd.DataFrame(table_8,columns = ["videotitle","videorelease","channelname"])
    st.write(df7)

elif question == "9. Average duration of all videos in each channel":
    query_9 = '''select channel_name as channelname, AVG(Duration) as averageduration from videos group by channel_name'''
    cursor.execute(query_9)
    my_database.commit()
    table_9 = cursor.fetchall()
    df8 = pd.DataFrame(table_9,columns = ["channelname","averageduration"])
    
    T9 = []
    for index,row in df8.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))

    df_AVG = pd.DataFrame(T9)
    st.write(df_AVG)

elif question == "10. Videos with highest number of comments":
    query_10 = '''select title as videotitle, channel_name as channelname, Comments as comments from videos 
                    where comments is not null order by comments desc '''
    cursor.execute(query_10)
    my_database.commit()
    table_10 = cursor.fetchall()
    df9 = pd.DataFrame(table_10,columns = ["videotitle","channelname","comments"])
    st.write(df9)