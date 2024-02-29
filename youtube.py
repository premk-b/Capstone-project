from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
from dateutil import parser


def Api_connect():
    Api_id = "AIzaSyAAehkmxb-LJEkfDNlq7h0_d9Zb-f9rOjQ"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey= Api_id)
    
    return(youtube)
youtube = Api_connect()

#channel information:
def get_channel_info(channel_id):
    request = youtube.channels().list(
                            part="snippet,contentDetails,statistics",
                            id= channel_id
    )
    response= request.execute()

    for i in response['items']:
        data=dict(Channel_Name = i['snippet']['title'],
                    Channel_Id = i['id'],
                    Subscribers = i['statistics']['subscriberCount'],
                    Channel_views= i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_description= i['snippet']['description'],
                    Playlist_Id= i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#Getting vedio ids from the channel
def get_videos_ids(channel_id):
    video_ids=[]
    response= youtube.channels().list(id= channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1= youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId= Playlist_Id,
                                                maxResults=50, 
                                                pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids

#Getting video information
def get_video_details(video_IDS):
        video_data=[]
        for video_id in video_IDS:
                request = youtube.videos().list(
                                part="snippet,contentDetails,statistics",
                                id= video_id )
                response= request.execute()
                                                                                        
                for item in response['items']:
                        data=dict(Channel_Name=item['snippet']['channelTitle'],
                                  Channel_Id=item['snippet']['channelId'],
                                Video_id=item['id'],
                                Video_name=item['snippet']['title'],
                                Video_description=item['snippet']['description'],
                                Published_At=item['snippet']['publishedAt'],
                                View_count=item['statistics']['viewCount'],
                                Like_count=item['statistics']['likeCount'],
                                Favourite_count=item['statistics']['favoriteCount'],
                                Duration=item['contentDetails']['duration'],
                                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                                Caption_status=item['contentDetails']['caption'],
                                Tags= item['snippet'].get('tags'),
                                Comment_count=item['statistics'].get('commentCount'),
                                )                
                        video_data.append(data)
        return video_data


#Getting playlist id 
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                response=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token)
                                                
                request=response.execute()

                for item in request['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                next_page_token=request.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data

#Getting comments information from youtube
def get_comment_info(video_IDS):
    comment_data=[]
    try:
        for video_id in video_IDS:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId= video_id,
                maxResults=50
            )
            response= request.execute()
            for item in response['items']:
                data=dict(Comments_id=item['snippet']['topLevelComment']['id'],
                        Video_ID=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_publishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                comment_data.append(data)

    except:
        pass
    return comment_data

#uploading data to mongo db
client =pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details= get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_details(vi_ids)
    com_details=get_comment_info(vi_ids)

    collect=db["channel_details"]
    collect.insert_one({"channel_information":ch_details,"video_information": vi_details,
                        "playlist_information":pl_details,"comment_information":com_details})
    
    return "uploaded successfully"

# Mysql connetion
def channel_table():
    config = {
        'user':'root', 'password':'Premk@1212',
        'host':'127.0.0.1', 'database':'youtube_data'
        }
    mysql_connection=mysql.connector.connect(**config)
    my_connection=mysql_connection.cursor()

    create_table='''create table if not exists channels(Channel_Name varchar(100),
                                                Channel_Id varchar(80), primary key(channel_id),
                                                Subscribers bigint,
                                                Channel_views bigint,
                                                Total_Videos int,
                                                Channel_description text,
                                                Playlist_Id varchar(80))'''
    my_connection.execute(create_table)
    mysql_connection.commit()
    channel_list=[]
    db=client["youtube_data"]
    channel_collection=db["channel_details"]
    for ch_data in channel_collection.find({},{"_id":0,"channel_information":1}):   
            channel_list.append(ch_data["channel_information"])
    df=pd.DataFrame(channel_list)

    for index,row in df.iterrows():
            insert_data=''' insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Channel_views,
                                                Total_Videos,
                                                Channel_description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Channel_views'],
                    row['Total_Videos'],
                    row['Channel_description'],
                    row['Playlist_Id'])
            try:
                my_connection.execute(insert_data,values)
                mysql_connection.commit()
            except:
                print("channels values are inserted")


def playlists_table():
    config = {
                    'user':'root', 'password':'Premk@1212',
                    'host':'127.0.0.1', 'database':'youtube_data'
                }
    mysql_connection=mysql.connector.connect(**config)
    my_connection=mysql_connection.cursor()

    create_table='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            video_Count int)'''

    my_connection.execute(create_table)
    mysql_connection.commit()

    play_list=[]
    db=client["youtube_data"]
    playlist_collection=db["channel_details"]
    for playlist_data in playlist_collection.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(playlist_data["playlist_information"])):
                play_list.append(playlist_data["playlist_information"][i])
    df1=pd.DataFrame(play_list)
    df1['PublishedAt'] = df1['PublishedAt'].apply(parser.parse)

    for index,row in df1.iterrows():
                playlists_data=''' insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                video_Count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
    
                values=(row['Playlist_Id'],
                                row['Title'],
                                row['Channel_Id'],
                                row['Channel_Name'],            
                                row['PublishedAt'],
                                row['video_Count']
                                )

                try:
                        my_connection.execute(playlists_data,values)
                        mysql_connection.commit()
                except:
                        print("channels values are inserted:",index)


def video_table():
    config = {
                'user':'root', 'password':'Premk@1212',
                'host':'127.0.0.1', 'database':'youtube_data'
            }
    mysql_connection=mysql.connector.connect(**config)
    my_connection=mysql_connection.cursor()

    create_videos='''create table if not exists Videos(Channel_Name varchar(100),
                                            Channel_Id varchar(80),
                                            Video_id varchar(100), primary key(video_id),
                                            Video_name varchar(200),
                                            Video_description text,
                                            Published_At varchar(200),
                                            View_count bigint,
                                            Like_count bigint,
                                            Favourite_count int,
                                            Duration varchar(100),
                                            Thumbnail text,
                                            Caption_status varchar(150),
                                            Tags text,
                                            Comment_count int);'''
    my_connection.execute(create_videos)
    mysql_connection.commit()
    video_list = []
    db = client["youtube_data"]
    video_collection = db["channel_details"]
    for video_data in video_collection.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])

    df2 = pd.DataFrame(video_list)
    df2['Duration'] = pd.to_timedelta(df2['Duration'])
    df2['Duration'] = df2['Duration'].astype(str)
    df2['Duration'] = df2['Duration'].str.extract(r'(\d+:\d+:\d+)')
    df2['Published_At'] = df2['Published_At'].apply(parser.parse)

    for index,row in df2.iterrows():
                videos_data=''' insert into videos(Channel_Name,
                                        Channel_Id,
                                        Video_id,
                                        Video_name,
                                        Video_description,
                                        Published_At,
                                        View_count,
                                        Like_count,
                                        Favourite_count,
                                        Duration,
                                        Thumbnail,
                                        Caption_status,
                                        Tags,
                                        Comment_count)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
                values1=(row['Channel_Name'],
                                row['Channel_Id'],
                                row['Video_id'],
                                row['Video_name'],
                                row['Video_description'],
                                row['Published_At'],
                                row['View_count'],
                                row['Like_count'],
                                row['Favourite_count'],
                                str(row['Duration']),
                                str(row['Thumbnail']),
                                row['Caption_status'],
                                str(row['Tags']),
                                row['Comment_count'])

                try:
                        my_connection.execute(videos_data,values1)
                        mysql_connection.commit()
                except:
                        print("channels values are inserted:",index)



def comment_table():

    config = {
            'user':'root', 'password':'Premk@1212',
            'host':'127.0.0.1', 'database':'youtube_data'
        }
    mysql_connection=mysql.connector.connect(**config)
    my_connection=mysql_connection.cursor()

    create_comments='''create table if not exists comments(Comments_id varchar(100) primary key,
                                Video_ID varchar(100),
                                Comment_text text,
                                Comment_Author varchar(200),
                                Comment_publishedAt timestamp
                                )'''

    my_connection.execute(create_comments)
    mysql_connection.commit()

    comment_list=[]
    db=client["youtube_data"]
    comment_collection=db["channel_details"]
    for comment_data in comment_collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])

    df3=pd.DataFrame(comment_list)
    df3['Comment_publishedAt'] = df3['Comment_publishedAt'].apply(parser.parse)
    for index,row in df3.iterrows():
                insert_comment=''' insert into comments(Comments_id,
                                                            Video_ID,
                                                            Comment_text,
                                                            Comment_Author,
                                                            Comment_publishedAt)
                                                            
                                                            values(%s,%s,%s,%s,%s)'''

                values2=(row['Comments_id'],
                                        row['Video_ID'],
                                        row['Comment_text'],
                                        row['Comment_Author'],
                                        row['Comment_publishedAt']
                                        )

                try:
                        my_connection.execute(insert_comment,values2)
                        mysql_connection.commit()
                except:
                        print("comments values are inserted")



def tables():
        channel_table()
        playlists_table()
        video_table()
        comment_table()
        return "Tables created successfully"

def show_channels_table():
    channel_list=[]
    db=client["youtube_data"]
    channel_collection=db["channel_details"]
    for ch_data in channel_collection.find({},{"_id":0,"channel_information":1}):   
        channel_list.append(ch_data["channel_information"])
    df=st.dataframe(channel_list)
    return df

def show_playlist_table():
    play_list=[]
    db=client["youtube_data"]
    playlist_collection=db["channel_details"]
    for playlist_data in playlist_collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
                play_list.append(playlist_data["playlist_information"][i])
    df1=st.dataframe(play_list)
    return df1

def show_video_table():
    video_list=[]
    db=client["youtube_data"]
    video_collection=db["channel_details"]
    for video_data in video_collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
                video_list.append(video_data["video_information"][i])
    df2=st.dataframe(video_list)
    return df2

def show_comment_table():
    comment_list=[]
    db=client["youtube_data"]
    comment_collection=db["channel_details"]
    for comment_data in comment_collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    df3=st.dataframe(comment_list)
    return df3

#streamlit data
with st.sidebar:
        st.title(":rainbow[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
        st.header(":rainbow[Data Harvesting from youtube]")
        st.header(":rainbow[Data storage in Mongo]")
        st.header(":rainbow[Data migration to MySql]")
        st.header(":rainbow[Streamlit the Data]")

channel_id=st.text_input(":red[Enter the channel ID]")

if st.button(":rainbow[Collect and Store Data]"):
        ch_ids=[]
        db=client["youtube_data"]
        channel_collection=db["channel_details"]
        for ch_data in channel_collection.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel_id in ch_ids:
                st.success("Channel Details of the given channel id already exists")  
        else:
                insert=channel_details(channel_id) 
                st.success(insert)   
if st.button(":rainbow[Migrate to Sql]"):
        Table=tables()
        st.succces(Table)

ch_names = []

db=client["youtube_data"]
channel_collection=db["channel_details"]
for ch_data in channel_collection.find({}, {"_id": 0, "channel_information": 1}):
    ch_names.append(ch_data["channel_information"]["Channel_Name"])
selected_channel = st.selectbox(":red[Select the channel]", ch_names[::])
print(selected_channel)

show_table = st.radio(":red[SELECT THE TABLE FOR VIEW]",["CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"])

if show_table=="CHANNELS":
    show_channels_table()
elif show_table=="PLAYLISTS":
      show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
      show_comment_table()



# Sql connections
            
config = {
    'user':'root', 'password':'Premk@1212',
    'host':'127.0.0.1', 'database':'youtube_data'
}
mysql_connection=mysql.connector.connect(**config)
my_connection=mysql_connection.cursor()

question=st.selectbox(":red[Select your questions]",["1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"])

if question== "1. What are the names of all the videos and their corresponding channels?":
      write='''select Video_name, Channel_Name from videos;'''
      my_connection.execute(write)
      question1=my_connection.fetchall()
      df=pd.DataFrame(question1,columns=["video title","Channel_Name"])
      st.write(df)    

elif question== "2. Which channels have the most number of videos, and how many videos do they have?":
      write='''select Channel_Name, Total_Videos from channels order by Total_Videos desc;'''
      my_connection.execute(write)
      question2=my_connection.fetchall()
      df1=pd.DataFrame(question2,columns=["Channel Name","Total videos"])
      st.write(df1)

elif question== "3. What are the top 10 most viewed videos and their respective channels?":
      write='''select View_count, Channel_Name, Video_name from videos order by View_count desc limit 10;'''
      my_connection.execute(write)
      question3=my_connection.fetchall()
      df2=pd.DataFrame(question3,columns=["View Count","Channel Name","Video Name"])
      st.write(df2)

elif question== "4. How many comments were made on each video, and what are their corresponding video names?":
      write='''select Comment_count, Video_name from videos;'''
      my_connection.execute(write)
      question4=my_connection.fetchall()
      df3=pd.DataFrame(question4,columns=["Total comments","Video Name"])
      st.write(df3)    


elif question== "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
      write='''select Like_count, Channel_Name from videos order by Like_count desc;'''
      my_connection.execute(write)
      question5=my_connection.fetchall()
      df4=pd.DataFrame(question5,columns=["Like count ","Channel Name"])
      st.write(df4)

elif question== "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
      write='''select Like_count, Video_name from videos order by Like_count desc;'''
      my_connection.execute(write)
      question5=my_connection.fetchall()
      df5=pd.DataFrame(question5,columns=["Like count ","Video Name"])
      st.write(df5)


elif question== "7. What is the total number of views for each channel, and what are their corresponding channel names?":
      write='''select Channel_views, Channel_Name from channels order by Channel_views desc;'''
      my_connection.execute(write)
      question7=my_connection.fetchall()
      df6=pd.DataFrame(question7,columns=["Total Views ","Channel Name"])
      st.write(df6)

elif question== "8. What are the names of all the channels that have published videos in the year 2022?":
      write='''select Channel_Name, Published_At from videos where extract(year from Published_At)=2022'''
      my_connection.execute(write)
      question8=my_connection.fetchall()
      df7=pd.DataFrame(question8,columns=["Channel Name ","Published Year"])
      st.write(df7)

elif question== "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
      write='''select Channel_Name, avg(Duration) as Average_Duration from videos group by Channel_Name'''
      my_connection.execute(write)
      question9=my_connection.fetchall()
      df8=pd.DataFrame(question9,columns=["Channel Name","Average Duration"])
      st.write(df8)

elif question== "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
      write='''select Comment_count, Video_name, Channel_Name from videos order by Comment_count desc;'''
      my_connection.execute(write)
      question10=my_connection.fetchall()
      df9=pd.DataFrame(question10,columns=["Comment Count","Video Name","Channel Name"])
      st.write(df9)