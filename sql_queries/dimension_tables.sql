-- Create the date dimension table
CREATE TABLE dim_date (
    date_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    date_day DATE,
    year_number INTEGER,
    month_of_year INTEGER,
    month_name_short VARCHAR(10),
    day_of_year INTEGER,
    day_of_week_name VARCHAR(10),
    quarter_of_year INTEGER
);

-- Create the time dimension table
CREATE TABLE dim_time (
    time_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- Create the location dimension table
CREATE TABLE dim_location (
    location_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(50),
    state VARCHAR(50),
    street VARCHAR(100),
    start_lat FLOAT,
    start_lng FLOAT,
    is_crossing BOOLEAN,
    is_railway BOOLEAN,
    is_traffic_calming BOOLEAN
);

-- Create the weather dimension table
CREATE TABLE dim_weather (
    weather_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    wind_chill FLOAT,
    wind_speed FLOAT,
    wind_direction VARCHAR(10),
    pressure FLOAT,
    humidity FLOAT,
    weather_condition VARCHAR(50),
    temperature FLOAT
);