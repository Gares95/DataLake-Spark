# Data Lake with Spark
***
This project will include the files neccessary to build an ETL pipeline for a *Data Lake* hosted on S3. The objective of this project is to succesfully extract data from an S3 bucket and to process it with Spark to load them back into another S3 bucket as a set of dimensional tables. This Spark process will be deployed on a cluster using *AWS*.

This repository simulates the creation of an ETL pipeline for a music streaming startup whose data resides in S3 and want to move their data warehouse to a data lake.

The data currently is in an S3 bucket in directories which contains their log data and song data in JSON files. The objective of the main program of this repository is to process the data and create a star schema optimized for queries for the song play analysis.


### Credits
Udacity provided the template and the guidelines to start this project.
The completion of this was made by Guillermo Garcia and the review of the program and the verification that the project followed the proper procedures was also made by my mentor from udacity.

# Data Files
***
The datasets used for this project that reside in S3 are:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. Here is an example of a filepath: _"song_data/A/B/C/TRABCEI128F424C983.json"_
And here is an example of one of the json files: _{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}_

### Log Dataset
The second dataset consists of log files in JSON format generated by this _event simulator_ based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.
Here is an example of a filepath: _"log_data/2018/11/2018-11-12-events.json"_
And here is an example of a json file for these events: _{"artist": "None", "auth": "Logged In", "gender": "F", "itemInSession": 0, "lastName": "Williams", "length": "227.15873", "level": "free", "location": "Klamath Falls OR", "method": "GET", "page": "Home", "registration": "1.541078e-12", "sessionId": "438", "Song": "None", "status": "200", "ts": "15465488945234", "userAgent": "Mozilla/5.0(WindowsNT,6.1;WOW641)", "userId": "53"}_

### The star schema tables
The star schema that is going to be created using this program will have the next structure:

- _Fact table_:
1. songplays [songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent]

- _Dimension tables_:
2. users [user_id, first_name, last_name, gender, level]
3. songs [song_id, title, artist_id, year, duration]
4. artist [artist_id, name, location, lattitude, longitude]
5. time [start_time, hour, day, week, month, year, weekday]

![alt text](https://raw.githubusercontent.com/Gares95/DataLake-Spark/master/Star%20Schema.PNG)

# Program files
***
## df.cfg

This file contains the AWS credentials to access the S3 buckets. 
Here you will have to introduce your AWS key and secret access key:

[AWS]

AWS_ACCESS_KEY_ID=<your AWS access key>
    
AWS_SECRET_ACCESS_KEY=<your AWS secret access key>

## etl.py

With this file we will process all files from the S3 buckets and create the star schema (with all the tables mentioned above) and will introduce them into a new S3 which will act as our Data Lake. 

## README.md

This file provides the descrpition of the program and process of the etl.

Some examples queries:
>In case I want to test how many songs in the "songplay list" do we have in our "song list"  
>%sql SELECT * FROM songplays WHERE song_id != 'None' LIMIT 5;


>If we want to check which are 'paid' level  
>%sql SELECT * FROM songplays WHERE level = 'paid';


>If we want to find a specific song by its songid  
>%sql SELECT * FROM songs WHERE song_id = 'SOZCTXZ12AB0182364';


>If we want to know which songs are from the year 2008  
>%sql SELECT * FROM songs WHERE year = '2008';

