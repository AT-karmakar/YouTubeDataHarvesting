YouTube Channel Data Analysis App:

This application is designed to analyze data from YouTube channels. It utilizes various technologies like Python, MongoDB, PostgreSQL, and Streamlit to fetch, process, and visualize information.

1. Establishing Connections:

The code starts by establishing connections to a PostgreSQL database and a MongoDB database. The PostgreSQL connection is used to store processed data, while the MongoDB connection is utilized for temporary storage and data harvesting from the YouTube API.

2. YouTube API Integration:

The app integrates with YouTube API using an API key, allowing it to fetch detailed information about channels, videos, comments, and statistics.

3. Data Retrieval and Processing:

The application contains functions to extract data from YouTube channels. It retrieves channel details, including subscriber count, total views, and video count. It also collects information about each video, such as view count, like count, comment count, and video duration. Data is organized into structured formats for further analysis.

4. Data Storage and Migration:

The app facilitates storing the collected data. It initially stores data in a MongoDB database and, upon user request, migrates this data to a PostgreSQL database for more structured and efficient storage.

5. User Interface:

The user interacts with the app through a Streamlit interface. They input a YouTube channel ID, and the app fetches and displays information about the channel and its videos. Users can visualize this data and perform various analyses using predefined SQL queries.

6. Data Analysis and Visualization:

The app supports several predefined analysis questions, such as finding top-viewed videos, videos with the most likes, and average video duration. These analyses are performed using SQL queries and displayed both in tabular form and visually, using pie charts, bar charts, or other appropriate visualizations.

7. Conclusion and Tools Used:

Finally, the app displays a summary of the tools used in its development: Python for coding, MongoDB and PostgreSQL for data storage, and Streamlit for creating the user interface.
