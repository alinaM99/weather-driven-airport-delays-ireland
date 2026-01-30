CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.weather_hourly_big (
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    timezone TEXT,
    temp_c DOUBLE PRECISION,
    feels_like_c DOUBLE PRECISION,
    humidity INTEGER,
    wind_speed DOUBLE PRECISION,
    clouds INTEGER,
    weather_main TEXT,
    weather_description TEXT,
    obs_time_utc TIMESTAMP,
    data_source TEXT,
    airport_code TEXT,
    severity_score INTEGER,
    delay_minutes INTEGER,
    is_delayed BOOLEAN,
    delay_bucket TEXT,
    delay_source TEXT
);
