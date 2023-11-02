

import streamlit as st
import psycopg2
from pymongo import MongoClient
from googleapiclient.discovery import build
from collections import OrderedDict
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image


# Establish a connection to PostgreSQL
with psycopg2.connect(
    dbname="YouTube",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
) as conn:
    with conn.cursor() as cur:
        # Database operations using `cur` here

# Establish a connection to MongoDB
        client = MongoClient("mongodb://localhost:27017")
        db = client["MyProjects_1"]
        collection = db["Youtube_dataharvesting"]

        # YouTube API setup
        api_key = "AIzaSyBInyOLc1UjNjVN9T8zuWQiAozbSy193cg"
        api_service_name = "youtube"
        api_version = "v3"
        youtube = build(api_service_name, api_version, developerKey=api_key)

# Define a function to get the video comments, for future call in main function
def get_video_comments(video_id):
    # Fetch comments for a video using commentThreads().list() API endpoint
    comments = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText"
    ).execute()

    comment_data = {}
    for comment in comments["items"]:
        comment_id = comment["id"]
        comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
        comment_author = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
        comment_published_at = comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]

        comment_data[comment_id] = {
            "Comment_Id": comment_id,
            "Comment_Text": comment_text,
            "Comment_Author": comment_author,
            "Comment_PublishedAt": comment_published_at
        }

    return comment_data


# Define a function for getting video statistics, for future call in main func
def get_video_statistics(video_id):
    # Fetch detailed video statistics using videos().list() API endpoint
    video_response = youtube.videos().list(
        part="statistics,contentDetails",
        id=video_id
    ).execute()

    if video_response['items']:
        statistics = video_response['items'][0]['statistics']
        view_count = int(statistics.get('viewCount', 0))
        like_count = int(statistics.get('likeCount', 0))
        dislike_count = int(statistics.get('dislikeCount', 0))
        favorite_count = int(statistics.get('favoriteCount', 0))
        comment_count = int(statistics.get('commentCount', 0))
        content_details = video_response['items'][0]['contentDetails']
        duration = content_details.get('duration', '') 

    else:
        # Set default values if no statistics available
        view_count, like_count, dislike_count, favorite_count, comment_count,duration = 0, 0, 0, 0, 0,''

    return view_count, like_count, dislike_count, favorite_count, comment_count, duration


# Define a function to get all the videos data of a channel which calls the upper two defined functions
def get_all_videos(channel_data):
    videos_data = OrderedDict()
    next_page_token = None

    try:
        while True:
            # Fetch videos from the channel's playlist 
            playlist_response = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=channel_data['Playlist_Id'],
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in playlist_response['items']:
                video_info = item['snippet']
                content_details = item['contentDetails']
                video_id = video_info['resourceId']['videoId']

                try:
                    playlist_name = video_info['playlistTitle']     # Retrieve playlist name from snippet
                except KeyError:
                    playlist_name = 'Not available'     # Set default value if playlist name is not present
                    
                view_count, like_count, dislike_count, favorite_count, comment_count, duration = get_video_statistics(video_id)
                video_data = OrderedDict({
                    "Video_Id": video_id,
                    "Video_Name": video_info['title'],
                    "Video_Description": video_info['description'],  
                    "Tags": video_info.get('tags', []),  
                    "PublishedAt": video_info['publishedAt'],
                    "Duration": duration,  
                    "Thumbnail": video_info['thumbnails']['default']['url'], 
                    "Statistics": {
                        "View_Count": view_count,
                        "Like_Count": like_count,
                        "Dislike_Count": dislike_count,
                        "Favorite_Count": favorite_count,
                        "Comment_Count": comment_count
                    },
                    "Caption_Status": content_details.get('caption', 'Not available'),  # Caption status
                    "Playlist_Name": playlist_name,  # Playlist name
                    "Comments": get_video_comments(video_id)
                })
                videos_data[video_id] = video_data

            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break  


    except Exception as e:
      print(f"An error occurred: {e}")

    return videos_data


# Define a main function which uses all the previous defined funcitons and gives us a consolidated result of a channels data
def get_channel_and_videos(channel_id):
    # Fetch channel details using channels().list() API endpoint
    try:
        channel_response = youtube.channels().list(
            part="snippet,contentDetails,statistics,status",
            id=channel_id
        ).execute()

        channel_info = channel_response['items'][0]['snippet']
        channel_status = channel_response['items'][0]['status']
        channel_data = {
            "Channel_Name": channel_info['title'],
            "Channel_Id": channel_id,
            "Subscription_Count": channel_response['items'][0]['statistics']['subscriberCount'],
            "Channel_Views": channel_response['items'][0]['statistics']['viewCount'],
            "Channel_Description": channel_info['description'],
            "Total_Videos" :channel_response['items'][0]['statistics']["videoCount"],
            "Playlist_Id": channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            "Channel_Type": channel_status.get('privacyStatus', 'Not available'), 
            "Channel_Status": channel_status.get('longUploadsStatus', 'Not available'),  
            "Country": channel_info.get('country', 'Not available')
        }

        videos_data = get_all_videos(channel_data)
        
    
        return channel_data, videos_data
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None, None

# Define a function to insert the retreived data into MongoDB 
def insert_to_mongodb(channel_data,videos_data):
    try:
        channel_and_videos_data = {
            "Channel_Name": channel_data,
            "Videos_data": videos_data
        }
        collection.insert_one(channel_and_videos_data)
    except Exception as e:
        st.error(f"An error occurred while inserting data into MongoDB: {e}")

# Create a cursor object
cur = conn.cursor()

# Define a similar function to migrate that data from MongoDB to SQL database
def migrate_to_sql():
    try:
        # Fetch data from MongoDB
        mongo_data = collection.find()

        # Iterate over for all the docs in MongoDB
        for document in mongo_data:
            channel_data = document.get("Channel_Name", {})
            videos_data = document.get("Videos_data", {})
            
            cur.execute('SELECT * FROM channel WHERE channel_id = %s', (channel_data['Channel_Id'],))
            existing_channel = cur.fetchone()

            # If the channel_id already exists, skip this record
            if existing_channel:
                continue
            else:        
                cur.execute('''
                    INSERT INTO channel (channel_id, channel_name,channel_type, channel_views, channel_description, channel_status, total_videos, playlist_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (channel_data['Channel_Id'], channel_data['Channel_Name'],channel_data['Channel_Type'], channel_data['Channel_Views'],
                    channel_data['Channel_Description'], channel_data['Channel_Status'], channel_data['Total_Videos'],channel_data['Playlist_Id']))
                # Insert data into playlist table and video table
            
            for video_id, video_info in videos_data.items():
                # Check if the playlist_id already exists in the playlist table
                cur.execute('SELECT * FROM playlist WHERE playlist_id = %s', (channel_data['Playlist_Id'],))
                existing_playlist = cur.fetchone()
            
                if existing_playlist:
                    pass
                else:
                    # Insert the playlist data into the playlist table
                    cur.execute('''
                        INSERT INTO playlist (playlist_id, channel_id, playlist_name)
                        VALUES (%s, %s, %s)
                    ''', (channel_data['Playlist_Id'], channel_data['Channel_Id'], video_info['Playlist_Name']))
            
                cur.execute('SELECT * FROM video WHERE video_id = %s', (video_id,))
                existing_video = cur.fetchone()
                
                if existing_video:
                    # Video with the same video_id already exists, skip this record
                    continue
                else:
                
                # Insert the video data into the video table
                    cur.execute('''
                        INSERT INTO video (video_id, playlist_id, video_name, video_description, published_date,
                                        view_count, like_count, dislike_count, favourite_count, comment_count,
                                        duration, thumbnail_url, caption_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (video_id, channel_data['Playlist_Id'], video_info['Video_Name'],video_info['Video_Description'] ,
                        video_info['PublishedAt'], video_info['Statistics']['View_Count'],
                        video_info['Statistics']['Like_Count'], video_info['Statistics']['Dislike_Count'],
                        video_info['Statistics']['Favorite_Count'], video_info['Statistics']['Comment_Count'],
                        video_info['Duration'], video_info['Thumbnail'], video_info['Caption_Status']))
            
                # Insert comments into comment table
                    for comment_id, comment_info in video_info['Comments'].items():
                        cur.execute('''
                            INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date)
                            VALUES (%s, %s, %s, %s, %s)
                        ''', (comment_id, video_id, comment_info['Comment_Text'], comment_info['Comment_Author'],
                            comment_info['Comment_PublishedAt']))
            
            # Commit the changes and close the connection
        conn.commit() 
    except Exception as e:
        st.error(f"An error occurred while migrating data to SQL: {e}")
    
# Building code for Streamlit
# Load the image
image_path = r"C:\Users\Akash\Downloads\YouTube-Logo.png"
try:
    original_image = Image.open(image_path)
    # Resize the image to a smaller dimension (adjust dimensions as needed)
    resized_image = original_image.resize((3480, 2460))
    st.image(resized_image, caption='', use_column_width=True)
except Exception as e:
    st.error(f"Error: {e}")
    st.write("Image failed to load. Please check the file path and format.")

# Streamlit app title
st.title("YouTube Channel Data Analysis")

session_state = st.session_state

# Use st.form to organize widgets and buttons
with st.form("channel_form"):
    # Get user input for YouTube channel ID
    new_channel_id = st.text_input("Enter YouTube Channel ID:")
    st.caption("###### Hint : Go to channel's home page > Right click > View page source > Find channel_id")
    # Button to add channel ID to the list and fetch details
    add_button = st.form_submit_button("Add Channel ID")

# Initialize channel_ids_list in session_state if not already initialized
if 'channel_ids_list' not in session_state:
    session_state['channel_ids_list'] = []

# Button to add channel ID to the list and fetch details
if add_button:
    if new_channel_id:
        session_state.channel_ids_list.append(new_channel_id)
        st.success(f"Channel ID {new_channel_id} added successfully in the list!")

# Dropdown to select channel ID from the list
selected_channel_id = st.selectbox("Select Channel ID:", session_state.channel_ids_list)

# Button to fetch and display channel data
if st.button("Fetch Channels Data"):
    with st.spinner('Fetching Channel Data, Please Wait...'):
        channel_id = selected_channel_id
        if channel_id:        
            channel_data, videos_data = get_channel_and_videos(channel_id)
            if channel_data and videos_data:
                with st.expander("Channel Information"):
                    st.write(f"Channel Name: {channel_data['Channel_Name']}")
                    st.write(f"Channel ID: {channel_data['Channel_Id']}")
                    st.write(f"Subscribers: {channel_data['Subscription_Count']}")
                    st.write(f"Views: {channel_data['Channel_Views']}")
                    st.write(f"Total Videos: {channel_data['Total_Videos']}")
                with st.expander("Videos Information"):
                    for video_id, video_info in videos_data.items():
                        st.subheader(f"Video ID: {video_id}")
                        st.write(f"Video Name: {video_info['Video_Name']}")
                        st.write(f"Published Date: {video_info['PublishedAt']}")
                        st.write(f"View Count: {video_info['Statistics']['View_Count']}")
                        st.write(f"Like Count: {video_info['Statistics']['Like_Count']}")
                        st.write(f"Comment Count: {video_info['Statistics']['Comment_Count']}")
            else:
                st.error("Invalid YouTube Channel ID. Please enter a valid ID.")
        else:
            st.warning("Please enter a YouTube Channel ID.")

# Button to insert data into MongoDB
if st.button("Insert Data into MongoDB"):
    with st.spinner('Inserting Data into MongoDB, Please Wait...'):
        channel_id = selected_channel_id
        if channel_id:
            channel_data, videos_data = get_channel_and_videos(channel_id)
            if channel_data and videos_data:
                insert_to_mongodb(channel_data, videos_data)
                st.success("Data inserted into MongoDB successfully.")
            else:
                st.warning("Fetch channel data first.")

# Button to migrate data from MongoDB to SQL
if st.button("Migrate Data to SQL"):
    with st.spinner('Migrating Data into SQL, Please Wait...'):
        migrate_to_sql()
        st.success("Data migrated to SQL successfully.")

# Dictionary containing questions and corresponding SQL queries
queries = {"":"",
    "1. What are the names of all the videos and their corresponding channels?":
        """SELECT v.video_name AS VideoName, 
        c.channel_name AS ChannelName FROM video v 
        JOIN channel c ON v.playlist_id = c.playlist_id;""",
    "2. Which channels have the most number of videos, and how many videos do they have?":
        """SELECT c.channel_name AS ChannelName, 
        COUNT(v.video_id) AS NumberOfVideos FROM channel c 
        JOIN video v ON c.playlist_id = v.playlist_id 
        GROUP BY c.channel_name ORDER BY NumberOfVideos DESC;""",
    "3. What are the top 10 most viewed videos and their respective channels?": 
        ("""
        SELECT v.video_name AS VideoName, v.view_count AS ViewCount, c.channel_name AS ChannelName
        FROM video v
        JOIN channel c ON v.playlist_id = c.playlist_id
        ORDER BY v.view_count DESC
        LIMIT 10;
        """,
        "Pie"  # Indicates that a plot is required for this question
    ),
    "4. How many comments were made on each video, and what are their corresponding video names?":
        """SELECT v.video_name AS VideoName, 
        COUNT(c.comment_id) AS CommentCount FROM video v 
        JOIN comment c ON v.video_id = c.video_id GROUP BY v.video_name;""",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        ("""SELECT v.video_name AS VideoName, v.like_count AS LikeCount, 
         c.channel_name AS ChannelName FROM video v 
         JOIN channel c ON v.playlist_id = c.playlist_id 
         ORDER BY v.like_count DESC LIMIT 10;""", 
         "Vertical"    # Indicates that a Vertical rep is required for this question
         ),
    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        """SELECT v.video_name AS VideoName, SUM(v.like_count) AS TotalLikes,
        SUM(v.dislike_count) AS TotalDislikes FROM video v 
        GROUP BY v.video_name;""",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        """SELECT c.channel_name AS ChannelName, SUM(v.view_count) AS TotalViews 
        FROM video v JOIN channel c ON v.playlist_id = c.playlist_id 
        GROUP BY c.channel_name;""",
    "8. What are the names of all the channels that have published videos in the year 2022?":
        """SELECT DISTINCT c.channel_name AS ChannelName 
        FROM video v JOIN channel c ON v.playlist_id = c.playlist_id 
        WHERE EXTRACT(YEAR FROM v.published_date::date) = 2022;""",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        ("""SELECT c.channel_name AS ChannelName, 
        AVG(EXTRACT(EPOCH FROM v.duration::INTERVAL)::integer) AS AverageDurationInSeconds 
        FROM video v JOIN channel c ON v.playlist_id = c.playlist_id 
        GROUP BY c.channel_name;""", 
        "Bar"   # Indicates that a Bar rep is required for this question
        ),
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        """SELECT v.video_name AS VideoName, 
        COUNT(c.comment_id) AS CommentCount, ch.channel_name AS ChannelName 
        FROM video v JOIN comment c ON v.video_id = c.video_id 
        JOIN channel ch ON v.playlist_id = ch.playlist_id GROUP BY v.video_name, 
        ch.channel_name ORDER BY CommentCount DESC LIMIT 10;""",
}

plot_types = {
    "3. What are the top 10 most viewed videos and their respective channels?": "Pie",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?": "Vertical",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":"Bar"
    }

selected_question = st.selectbox("Select any Analysis:", list(queries.keys()))

cur = conn.cursor()
# Button to fetch answers for selected questions
if st.button("Fetch Answer"):
    if selected_question in queries:
        query_info = queries[selected_question]
        if isinstance(query_info, tuple) and len(query_info) == 2:
            query, requires_plot = query_info
        else:
            query = query_info
            requires_plot = False

        # Execute the selected query
        cur.execute(query)
        result = cur.fetchall()

        if result:
            # Convert SQL query result to a pandas DataFrame
            df = pd.DataFrame(result, columns=[col[0] for col in cur.description])

            # Display the table with the query result
            st.table(df)

            # Generate the plot if required
            if requires_plot:
                plot_type = plot_types.get(selected_question)
                if plot_type == "Pie":
                    #Qstn 3
                    df3 = pd.DataFrame(result, columns=["VideoName", "ViewCount", "ChannelName"])
                    # Generate the plot using Matplotlib
                    # Data for the pie chart
                    video_names = df3['VideoName']
                    view_counts = df3['ViewCount']

                    # Colors for each video in the pie chart
                    colors = plt.cm.Paired(range(len(video_names)))

                    # Plotting using a pie chart
                    plt.figure(figsize=(8, 8))
                    plt.pie(view_counts, labels=video_names, autopct='%1.1f%%', startangle=140, colors=colors)
                    plt.title('View Count Distribution for Top 10 Most Viewed Videos')
                    st.pyplot(plt)  # Display the plot in Streamlit

                elif plot_type == "Vertical":
                    #Qstn 5
                    # Convert SQL query result to a pandas DataFrame
                    df5 = pd.DataFrame(result, columns=["VideoName", "LikeCount", "ChannelName"])
                    plt.figure(figsize=(10, 6))
                    plt.bar(df5['VideoName'], df5['LikeCount'], color='skyblue')
                    plt.ylabel('Like Count')
                    plt.xlabel('Video Name')
                    plt.title('Top 10 Videos with Highest Number of Likes and Their Channels')
                    plt.xticks(rotation=45)  # Rotate x-axis labels for better visibility
                    st.pyplot(plt)

                elif plot_type == "Bar":
                    #Qstn 10
                    # Plotting using a bar chart
                    df9 = pd.DataFrame(result, columns=["ChannelName", "AverageDurationInSeconds"])
                    plt.figure(figsize=(10, 6))
                    plt.bar(df9['ChannelName'], df9['AverageDurationInSeconds'], color='skyblue')
                    plt.xlabel('Channel Name')
                    plt.ylabel('Average Duration (Seconds)')
                    plt.title('Average Duration of Videos for Each Channel')
                    plt.xticks(rotation=45)  # Rotate x-axis labels for better visibility 
                    st.pyplot(plt)


        else:
            st.write("No results found for the selected question.")

    conn.close()

st.caption("Tools used in this app: Python, MongoDB, PostgreSQL, Streamlit")