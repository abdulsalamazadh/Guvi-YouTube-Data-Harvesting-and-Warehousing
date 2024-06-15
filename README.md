# YouTube Data Harvesting and Warehousing

## Overview
YouTube Data Harvesting and Warehousing is a project designed to provide users with the ability to access and analyze data from various YouTube channels. The project leverages SQL, MongoDB, and Streamlit to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.

### Tools and Libraries Used

# Streamlit
Streamlit is used to create a user-friendly interface that enables users to interact with the application and perform data retrieval and analysis tasks.

# Python
Python, known for its ease of learning and understanding, is the primary language used in this project. It is employed for the development of the entire application, including data retrieval, processing, analysis, and visualization.

# Google API Client
The googleapiclient library in Python facilitates communication with various Google APIs. In this project, it is primarily used to interact with YouTube's Data API v3, enabling the retrieval of essential information such as channel details, video specifics, and comments. This library allows developers to easily access and manipulate YouTube's extensive data resources programmatically.

# PostgreSQL
PostgreSQL is an open-source, advanced, and highly scalable database management system (DBMS). It is renowned for its reliability and extensive features, providing a robust platform for storing and managing structured data with advanced SQL capabilities.

## Required Libraries
1. googleapiclient.discovery
2. streamlit
3. psycopg2
4. pandas

## Setup
1. **Obtain YouTube API key:** Sign up for a YouTube API key from the Google Developer Console.

2. **Install dependencies:** Install the required Python packages using pip:
    ```
    pip install streamlit pandas sqlite3
    ```
3. **Clone the repository:** Clone this repository to your local machine:
    ```
    git clone https://github.com/yourusername/youtube-data-harvesting.git
    ```
4. **Configuration:** Replace the placeholder API key in the code with your YouTube API key.

5. **Run the Streamlit app:** Navigate to the project directory and run the Streamlit app:
    ```
    cd youtube-data-harvesting
    streamlit run app.py
    ```
6. **setting_up .env_templete:** Use your YouTube API Key, MongoDB link, and PostgreSQL credentials as specified in the .env_template file to run the app.

## Features
The YouTube Data Harvesting and Warehousing application offers the following functionalities:

Retrieval of channel and video data from YouTube using the YouTube API.

Migration of data from the data lake to a SQL database for efficient querying and analysis.

Search and retrieval of data from the SQL database using various search options.

This project provides a comprehensive solution for accessing, storing, and analyzing YouTube data efficiently and ethically.






