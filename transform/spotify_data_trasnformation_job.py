import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.sql.functions import explode, col, initcap, to_date
from datetime import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ["S3_RAW_PATH", "S3_TRANSFORMED_PATH", "BUCKET_NAME", "TO_BE_PROCESSED", "PROCESSED"])

s3_raw_path = args["S3_RAW_PATH"]
s3_transformed_path = args["S3_TRANSFORMED_PATH"]
bucket_name = args["BUCKET_NAME"]
to_be_processed_path = args["TO_BE_PROCESSED"]
processed_path = args["PROCESSED"]

# Read all the files stored in the s3_path
def get_s3_data(s3_path):
    return glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths":[s3_path]},
        format="json"
    )

# Extract album details
def process_album(df):
    album_df = df.select(
        col("items.track.album.id").alias("album_id"),
        initcap(col("items.track.album.name")).alias("album_name"),
        col("items.track.album.release_date").alias("album_release_date"),
        col("items.track.album.total_tracks").alias("album_total_tracks"),
        col("items.track.album.external_urls.spotify").alias("album_url"),
    ).drop_duplicates(["album_id"])
    
    #Convert string dates to actual dates
    album_df = album_df.withColumn("album_release_date", to_date(col("album_release_date")))
    
    return album_df

# Extract artist data
def process_artist(df):
    return df.select(explode("items.track.artists").alias("artist")).select(
        col("artist.id").alias("artist_id"),
        initcap(col("artist.name")).alias("artist_name"),
        col("artist.href").alias("external_href")
    ).drop_duplicates(["artist_id"])

# Extract song data
def process_song(df):
    song_df = df.select(
        col("items.track.id").alias("song_id"),
        initcap(col("items.track.name")).alias("song_name"),
        col("items.track.duration_ms").alias("song_duration"),
        col("items.track.external_urls.spotify").alias("song_url"),
        col("items.track.popularity").alias("song_popularity"),
        col("items.added_at").alias("song_added"),
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.artists")[0]["id"].alias("artist_id"),
    ).drop_duplicates(["song_id"])

    #Convert string dates to actual dates
    song_df = song_df.withColumn("song_added", to_date(col("song_added")))
    
    return song_df
    
# Upload transformed file to s3
def upload_to_s3(df, s3_path, path_suffix, format_type):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"{s3_path}/{path_suffix}/"},
        format=format_type
    )
 
# Lists all JSON files under the given prefix 
def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [obj['Key'] for obj in response.get("Contents", []) if obj['Key'].endswith(".json")]
    return keys
    
# Copies & deletes the files from source to destination   
def move_files_to_processed(bucket, to_be_processed_path, processed_path):
    s3_client = boto3.client('s3')
    keys = list_s3_objects(bucket, to_be_processed_path)
    for key in keys:
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        
        #replace the key
        new_key = key.replace(to_be_processed_path, processed_path)
        
        # copy the processed file to processed path
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket,
            Key=new_key
        )
        print(f"Copied file {key} to {new_key}")
        
        # delete the file in to_processed folder
        s3_client.delete_object(
            Bucket=bucket,
            Key=key
        )
        print(f"Deleted file {key} from {prefix}") 
 
 
spotify_dyf = get_s3_data(s3_raw_path)
spotify_raw_df = spotify_dyf.toDF()

# Explode the array of objects to individual columns
spotify_items_df = spotify_raw_df.withColumn("items", explode("items"))

album_df = process_album(spotify_items_df)
artist_df = process_artist(spotify_items_df)
song_df = process_song(spotify_items_df)

# Write into single file 
# album_df = album_df.coalesce(1)
# artist_df = artist_df.coalesce(1)
# song_df = song_df.coalesce(1)

# Upload to s3
upload_to_s3(album_df, s3_transformed_path, "albums/albums_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
upload_to_s3(artist_df, s3_transformed_path, "artists/artists_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
upload_to_s3(song_df, s3_transformed_path, "songs/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")

# Copy files from to_processed to processed & delete 
move_files_to_processed(bucket_name, to_be_processed_path, processed_path)

job.commit()