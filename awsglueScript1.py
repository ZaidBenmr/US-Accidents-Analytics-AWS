import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths
input_path = 's3://us-accidents-data/US_Accidents_March23.csv'
output_path = 's3://us-accidents-data/parquet_files/'

# Define the schema
schema = StructType([
    StructField('ID', StringType(), True),
    StructField('Source', StringType(), True),
    StructField('Severity', StringType(), True),
    StructField('Start_Lat', FloatType(), True),
    StructField('Start_Lng', FloatType(), True),
    StructField('Start_Time', StringType(), True),
    StructField('End_Time', StringType(), True),
    StructField('Weather_Timestamp', StringType(), True),
    StructField('Distance(mi)', FloatType(), True),
    StructField('Description', StringType(), True),
    StructField('Street', StringType(), True),
    StructField('City', StringType(), True),
    StructField('County', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Zipcode', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Airport_Code', StringType(), True),
    StructField('Temperature(F)', FloatType(), True),
    StructField('Wind_Chill(F)', FloatType(), True),
    StructField('Humidity(%)', FloatType(), True),
    StructField('Pressure(in)', FloatType(), True),
    StructField('Visibility(mi)', FloatType(), True),
    StructField('Wind_Direction', StringType(), True),
    StructField('Wind_Speed(mph)', FloatType(), True),
    StructField('Precipitation(in)', FloatType(), True),
    StructField('Weather_Condition', StringType(), True),
    StructField('Amenity', BooleanType(), True),
    StructField('Bump', BooleanType(), True),
    StructField('Crossing', BooleanType(), True),
    StructField('Give_Way', BooleanType(), True),
    StructField('Junction', BooleanType(), True),
    StructField('No_Exit', BooleanType(), True),
    StructField('Railway', BooleanType(), True),
    StructField('Roundabout', BooleanType(), True),
    StructField('Station', BooleanType(), True),
    StructField('Stop', BooleanType(), True),
    StructField('Traffic_Calming', BooleanType(), True),
    StructField('Traffic_Signal', BooleanType(), True),
    StructField('Turning_Loop', BooleanType(), True),
    StructField('Sunrise_Sunset', StringType(), True),
    StructField('Civil_Twilight', StringType(), True),
    StructField('Nautical_Twilight', StringType(), True),
    StructField('Astronomical_Twilight', StringType(), True)
])

# Read the CSV file into a DynamicFrame
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True}
)

# Convert DynamicFrame to DataFrame and apply the schema
df = datasource0.toDF()

# Apply the schema to the DataFrame
for field in schema.fields:
    df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

# Drop unnecessary columns
columns_to_drop = ['End_Lat', 'End_Lng', 'Timezone', 'Airport_Code', 'Weather_Timestamp', 
                   'Precipitation(in)', 'Amenity', 'Bump', 'Give_Way', 'Stop', 
                   'Roundabout', 'No_Exit', 'Junction', 'Turning_Loop', 
                   'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight', 'Zipcode']
df = df.drop(*columns_to_drop)

# Rename columns
columns_to_rename = {
    'Distance(mi)': 'Distance',
    'Precipitation(in)': 'Precipitation',
    'Wind_Speed(mph)': 'Wind_Speed',
    'Visibility(mi)': 'Visibility',
    'Pressure(in)': 'Pressure',
    'Humidity(%)': 'Humidity',
    'Wind_Chill(F)': 'Wind_Chill',
    'Temperature(F)': 'Temperature'
}
for old_name, new_name in columns_to_rename.items():
    df = df.withColumnRenamed(old_name, new_name)

# Create new columns for day and month
df = df.withColumn('Day_Of_Acc', F.dayofmonth(F.col('Start_Time')))
df = df.withColumn('Month_Of_Acc', F.month(F.col('Start_Time')))

# Convert 'Severity' column to numbers
df = df.withColumn('Severity', F.col('Severity').cast('int'))

# Drop null values
df = df.na.drop()

# Convert the "Start_Time" and "End_Time" columns to datetime with microsecond precision
df = df.withColumn('Start_Time', F.col('Start_Time').cast('timestamp'))
df = df.withColumn('End_Time', F.col('End_Time').cast('timestamp'))

# Group the DataFrame by year based on the "Start_Time" column
df = df.withColumn('Year', F.year(F.col('Start_Time')))
years = df.select('Year').distinct().collect()

# Save each year group as a Parquet file
for year in years:
    year_value = year['Year']
    df_year = df.filter(F.col('Year') == year_value)
    df_year.write.mode('overwrite').parquet(f'{output_path}partition_{year_value}.parquet')

# Commit the job
job.commit()
