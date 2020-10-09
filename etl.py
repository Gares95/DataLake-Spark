import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

def create_spark_session():
    """
    This function instantiate the spark module that we are going to use.

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function extracts and process the data from the S3 bucket from udacity 
	to create the "songs" and "artist" dimensional tables and insert them in the new
	S3 bucket.

    INPUTS:
    * spark: the spark module
    * input_data: the path to the S3 bucket that will be extracted and processed.
	+ output_data: the path to the new S3 bucket in which we will insert the tables.
    """
    # get filepath to song data file
    
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet(os.path.join(output_data, "songs"), mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id','artist_name as name','artist_location as location','artist_latitude as latitude','artist_longitude as longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists"), mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function extracts and process the data from the S3 bucket from udacity 
	to create the "users" and "time" dimensional tables and the "songplays" fact 
	table to later insert them in the new S3 bucket. 
	

    INPUTS:
    * spark: the spark module
    * input_data: the path to the S3 bucket that will be extracted and processed.
	+ output_data: the path to the new S3 bucket in which we will insert the tables.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr(['userId as user_id','firstName as first_name','lastName as last_name','gender','level']) 
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users"), mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (int(int(x)/1000.0)))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(['start_time','datetime']).withColumn('hour', hour('datetime')).withColumn('day', dayofmonth('datetime')).withColumn('week', weekofyear('datetime')).withColumn('month', month('datetime')).withColumn('year', year('datetime')).withColumn('weekday', dayofweek('datetime')).drop('datetime').dropDuplicates()
    time_table.write.partitionBy(['year','month']).parquet(os.path.join(output_data, "times"), mode = "overwrite")
    
    song_df = spark.read.parquet(os.path.join(input_data, "song_data/*/*/*/*.json"))

    df.createOrReplaceTempView("logs")
    song_df.createOrReplaceTempView("songs")
    songplays_table = spark.sql(
    '''
    SELECT monotonically_increasing_id() as songplay_id, logs.datetime as start_time, logs.userId as user_id, logs.level, songs.song_id, songs.artist_id, logs.sessionId, logs.location, logs.userAgent
    FROM logs
    JOIN songs 
    ON songs.title = logs.song AND songs.artist_name = logs.artist AND songs.duration = logs.length
    '''
    )
    songplays_table = songplays_table.withColumn('month', month('start_time')).withColumn('year', year('start_time'))
    songplays_table.write.partitionBy(['year','month']).parquet(os.path.join(output_data, "songplays"), mode = "overwrite")


def main():
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://gg-data-lake-udacity/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
