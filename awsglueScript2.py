import sys
import hashlib
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql import functions as F

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define input and output paths
athena_database = "us-accidents"
athena_table = "parquet_files"
redshift_jdbc_url = "jdbc:redshift://default-workgroup.000000000.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
redshift_username = "admin"
redshift_password = "password"
redshift_temp_dir = "s3://us-accidents/temp/"

# Load data from Athena
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=athena_database,
    table_name=athena_table
)

# Convert to DataFrame and select relevant columns
df = datasource0.toDF()

# Load dimension tables first
df_date = df.select("Start_Time") \
    .withColumn("date_day", F.to_date("Start_Time")) \
    .withColumn("year_number", F.year("Start_Time")) \
    .withColumn("month_of_year", F.month("Start_Time")) \
    .withColumn("month_name_short", F.date_format("Start_Time", "MMM")) \
    .withColumn("day_of_year", F.dayofyear("Start_Time")) \
    .withColumn("day_of_week_name", F.date_format("Start_Time", "EEEE")) \
    .withColumn("quarter_of_year", F.quarter("Start_Time")) \
    .drop("Start_Time") \
    .distinct() \
    .withColumn("date_id", monotonically_increasing_id())

df_time = df.select("Start_Time", "End_Time") \
    .distinct() \
    .withColumnRenamed("Start_Time", "start_time") \
    .withColumnRenamed("End_Time", "end_time") \
    .withColumn("time_id", monotonically_increasing_id())

def generate_surrogate_key(row):
    key_string = f"{row['Start_Lat']}{row['Start_Lng']}"
    return hashlib.md5(key_string.encode()).hexdigest()
    
generate_surrogate_key_udf = F.udf(generate_surrogate_key)

df_location = df.select("City", "State", "Street", "Start_Lat", "Start_Lng", "Crossing", "Railway", "Traffic_Calming") \
    .distinct() \
    .withColumnRenamed("City", "city") \
    .withColumnRenamed("State", "state") \
    .withColumnRenamed("Street", "street") \
    .withColumnRenamed("Start_Lat", "start_lat") \
    .withColumnRenamed("Start_Lng", "start_lng") \
    .withColumnRenamed("Crossing", "is_crossing") \
    .withColumnRenamed("Railway", "is_railway") \
    .withColumnRenamed("Traffic_Calming", "is_traffic_calming") \
    .withColumn("location_id", generate_surrogate_key_udf(F.struct("start_lat", "start_lng")))

df_weather = df.select("Wind_Chill", "Wind_Speed", "Wind_Direction", "Pressure", "Humidity", "Weather_Condition", "Temperature") \
    .distinct() \
    .withColumnRenamed("Wind_Chill", "wind_chill") \
    .withColumnRenamed("Wind_Speed", "wind_speed") \
    .withColumnRenamed("Wind_Direction", "wind_direction") \
    .withColumnRenamed("Pressure", "pressure") \
    .withColumnRenamed("Humidity", "humidity") \
    .withColumnRenamed("Weather_Condition", "weather_condition") \
    .withColumnRenamed("Temperature", "temperature") \
    .withColumn("weather_id", monotonically_increasing_id())

# Load data into Redshift
def load_to_redshift(dataframe, table_name):
    dataframe.write \
        .format("jdbc") \
        .option("url", redshift_jdbc_url) \
        .option("dbtable", table_name) \
        .option("tempdir", redshift_temp_dir) \
        .option("user", redshift_username) \
        .option("password", redshift_password) \
        .mode("append") \
        .save()

load_to_redshift(df_date, "dim_date")
load_to_redshift(df_time, "dim_time")
load_to_redshift(df_location, "dim_location")
load_to_redshift(df_weather, "dim_weather")

# Load fact table
fact_df = df.join(df_date, on=[df["Start_Time"].cast("date") == df_date["date_day"], df["Start_Time"].cast("year") == df_date["year_number"], df["Start_Time"].cast("month") == df_date["month_of_year"], df["Start_Time"].cast("dayofyear") == df_date["day_of_year"], F.date_format(df["Start_Time"], "EEEE") == df_date["day_of_week_name"], df["Start_Time"].cast("quarter") == df_date["quarter_of_year"]], how="left") \
    .join(df_time, [df["Start_Time"] == df_time["start_time"], df["End_Time"] == df_time["end_time"]], how="left") \
    .join(df_location, on=[df["City"] == df_location["city"], df["State"] == df_location["state"], df["Street"] == df_location["street"], df["Start_Lat"] == df_location["start_lat"], df["Start_Lng"] == df_location["start_lng"], df["Crossing"] == df_location["is_crossing"], df["Railway"] == df_location["is_railway"], df["Traffic_Calming"] == df_location["is_traffic_calming"]], how="left") \
   
load_to_redshift(fact_df, "fact_table")

# Commit job
job.commit()