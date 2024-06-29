-- Create the fact table
CREATE TABLE fact_table (
    accident_id VARCHAR PRIMARY KEY,
    severity INTEGER,
    date_id INTEGER,
    time_id INTEGER,
    location_id INTEGER,
    weather_id INTEGER,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (weather_id) REFERENCES dim_weather(weather_id)
);