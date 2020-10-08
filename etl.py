import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file

    # for local testing
    # song_data = "data/song_data/*/*/*/*.json"
    
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet("{}/songs.parquet".format(output_data), mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id','artist_name as name','artist_location as location','artist_latitude as latitude','artist_longitude as longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet("{}/artists.parquet".format(output_data), mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data ="{}/log_data/*.json".format(input_data)
    
    # For local testing
    # log_data = "data/log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr(['userId as user_id','firstName as first_name','lastName as last_name','gender','level']) 
    
    # write users table to parquet files
    users_table.write.parquet("{}/users.parquet".format(output_data), mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (int(int(x)/1000.0)))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(['start_time','datetime']).withColumn('hour', hour('datetime')).withColumn('day', dayofmonth('datetime')).withColumn('week', weekofyear('datetime')).withColumn('month', month('datetime')).withColumn('year', year('datetime')).withColumn('weekday', dayofweek('datetime')).drop('datetime').dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet("{}/times.parquet".format(output_data), mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))

    # extract columns from joined song and log datasets to create songplays table 
    df.createOrReplaceTempView("logs")
    song_df.createOrReplaceTempView("songs")
    songplays_table = spark.sql(
    '''
    SELECT monotonically_increasing_id() as songplay_id, logs.datetime as start_time, logs.userId as user_id, logs.level, songs.song_id, songs.artist_id, logs.sessionId, logs.location, logs.userAgent
    FROM logs
    JOIN songs 
    ON songs.title = logs.song
    '''
    )
    songplays_table = songplays_table.withColumn('month', month('start_time')).withColumn('year', year('start_time'))
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).parquet("{}/songplays.parquet".format(output_data), mode = "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://gg-data-lake-udacity/"

  # For local testing
  # input_data = "data"  
  # output_data = "output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
