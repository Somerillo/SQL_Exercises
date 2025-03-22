-- -------------------------------------------------------------------------------
-- 			DATA CLEANING
-- -------------------------------------------------------------------------------

-- general view
SELECT * FROM globalweatherrepository
    ORDER BY country, location_name, last_updated;

-- find missing values in main columns
/* the dataset is very well curated and doesnt have null or missing values
anyway we still proceed with the most important columns */
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN location_name IS NULL THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) AS null_lat,
    SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END) AS null_long,
    SUM(CASE WHEN timezone IS NULL THEN 1 ELSE 0 END) AS null_timez,
    SUM(CASE WHEN last_updated IS NULL THEN 1 ELSE 0 END) AS null_local_time,
    SUM(CASE WHEN temperature_celsius IS NULL THEN 1 ELSE 0 END) AS null_temp,
    SUM(CASE WHEN condition_text IS NULL THEN 1 ELSE 0 END) AS null_condition_text,
    SUM(CASE WHEN wind_kph IS NULL THEN 1 ELSE 0 END) AS null_wind_spd,
    SUM(CASE WHEN pressure_mb IS NULL THEN 1 ELSE 0 END) AS null_pressure,
    SUM(CASE WHEN precip_mm IS NULL THEN 1 ELSE 0 END) AS null_precip,
    SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END) AS null_humid
FROM globalweatherrepository;

-- check for inconsistent values
SELECT
    country,
    last_updated,
    temperature_celsius,
    humidity,
    wind_kph,
    precip_mm,
    pressure_mb
FROM globalweatherrepository
WHERE temperature_celsius < -89.2 OR temperature_celsius > 60
    OR humidity < 0 OR humidity > 100
    OR precip_mm < 0
    OR pressure_mb < 870 OR pressure_mb > 1090;

-- there are inconsistent windspeed values, 
/* For sustained wind speed (wind_kph):
    potentially extreme: > 60 km/h (strong gale)
    likely error: > 120 km/h (hurricane force)
*/
SELECT
    country,
    location_name,
    last_updated,
    wind_kph
FROM globalweatherrepository
WHERE wind_kph < 0 OR wind_kph > 60;

-- we flag the extreme windspeeds in a temporary table to not alter the original dataset
/* in Oslo it might be possible to have windspeeds over 60 kph, and those values should be checked */
CREATE TEMPORARY TABLE temp_flag_wind_speed AS
SELECT
    *,
    CASE
        WHEN wind_kph > 120 THEN 'extreme (likely error)'
        WHEN wind_kph > 60 THEN 'high (potentially extreme event)'
    END AS wind_speed_flag
FROM globalweatherrepository
WHERE wind_kph > 60;

SELECT country, location_name, last_updated, wind_kph FROM temp_flag_wind_speed
ORDER BY country, last_updated;


-- identifying non-latin characters
SELECT country, location_name FROM globalweatherrepository
WHERE country REGEXP '[^ -~]'
    OR location_name REGEXP '[^ -~]';

CREATE TEMPORARY TABLE flag_temp_country_names (
    country VARCHAR(255),
    translated_country VARCHAR(255)
);

INSERT INTO flag_temp_country_names (country, translated_country)
VALUES
	('Malásia', 'Malaysia'),
    ('كولومبيا', 'Colombia'),
    ('Гватемала', 'Guatemala'),
    ('Польша', 'Poland'),
    ('Polônia', 'Poland'),
    ('Турция', 'Turkey'),
    ('Südkorea', 'South Korea'),
    ('Bélgica', 'Belgium'),
    ('Turkménistan', 'Turkmenistan'),
    ('火鸡', 'Turkey');

/* from this we can see the natural key we must use is location_name and not the country
which derives in the next step: */


-- handling duplicates, this section extends a little
/* there are locations where the data was taken twice the same day
also all these duplicates don't contain any of the windspeed outliers
*/
SELECT
    location_name,
    DATE(last_updated) AS update_date,
    COUNT(*) AS duplicated_number
FROM globalweatherrepository
GROUP BY location_name, DATE(last_updated)
HAVING
    COUNT(*) > 1
ORDER BY duplicated_number, update_date, location_name;


/* we'll create a new temporary table flagging those rows
and taking average of the numerical values
we'll also take only the relevant columns for the view */
CREATE TEMPORARY TABLE temp_flag_duplicates_avg AS
SELECT
    country,
    location_name,
    latitude,
    longitude,
    -- we combine date and average time to form a new datetime for last_updated
    DATE(MIN(last_updated)) + INTERVAL AVG(TIME_TO_SEC(TIME(last_updated))) SECOND AS last_updated,
    AVG(temperature_celsius) AS temperature_celsius,
    AVG(wind_kph) AS wind_kph,
    AVG(pressure_mb) AS pressure_mb,
    AVG(precip_mm) AS precip_mm,
    AVG(humidity) AS humidity,
    AVG(air_quality_Carbon_Monoxide) AS air_quality_Carbon_Monoxide,
    AVG(air_quality_Ozone) AS air_quality_Ozone,
    AVG(air_quality_Nitrogen_dioxide) AS air_quality_Nitrogen_dioxide,
    AVG(air_quality_Sulphur_dioxide) AS air_quality_Sulphur_dioxide,
    AVG(`air_quality_PM2.5`) AS `air_quality_PM2.5`,  -- keep original name for consistency
    AVG(air_quality_PM10) AS air_quality_PM10,  -- keep original name for consistency
    AVG(`air_quality_us-epa-index`) AS `air_quality_us-epa-index`,  -- keep original name for consistency
    AVG(`air_quality_gb-defra-index`) AS `air_quality_gb-defra-index`  -- keep original name for consistency
FROM globalweatherrepository
GROUP BY
    country,
    location_name,
    latitude,
    longitude,
    DATE(last_updated)
HAVING
    COUNT(*) > 1;


-- now we create the cleaned table
/* first we create the view without duplicates;
to avoid disable and enable safe updates we
create the table avoiding conflicting values
*/
/* the outliers we found are in the wind speed,
but not in humidity, temperature, or pressure;
if we weren't to take the wind speed values we could
keep the outliers
*/
CREATE TEMPORARY TABLE temp_cleaned_globalweather AS
SELECT
    g.country,
    g.location_name,
    g.latitude,
    g.longitude,
    g.last_updated,
    g.temperature_celsius,
    g.wind_kph,
    g.pressure_mb,
    g.precip_mm,
    g.humidity,
    g.air_quality_Carbon_Monoxide,
    g.air_quality_Ozone,
    g.air_quality_Nitrogen_dioxide,
    g.air_quality_Sulphur_dioxide,
    g.`air_quality_PM2.5`,
    g.air_quality_PM10,
    g.`air_quality_us-epa-index`,
    g.`air_quality_gb-defra-index`
FROM globalweatherrepository g
-- we avoid duplicated values:
LEFT JOIN (
    SELECT location_name, DATE(last_updated) AS update_date
    FROM temp_flag_duplicates_avg
) d ON g.location_name = d.location_name AND DATE(g.last_updated) = d.update_date
-- we avoid wind speed outliers:
LEFT JOIN (
    SELECT location_name, DATE(last_updated) AS update_date
    FROM temp_flag_wind_speed
) ws ON g.location_name = ws.location_name AND DATE(g.last_updated) = ws.update_date
WHERE d.location_name IS NULL AND ws.location_name IS NULL;

DROP TEMPORARY TABLE temp_flag_wind_speed;

-- we add the averaged values to the cleaned table
INSERT INTO temp_cleaned_globalweather
SELECT *
FROM temp_flag_duplicates_avg;

DROP TEMPORARY TABLE temp_flag_duplicates_avg;



-- finally for the data cleaning we replace non latin country names
CREATE TABLE cleaned_globalweather AS
SELECT
    -- replace with translated country name if available, otherwise keep original
    COALESCE(f.translated_country, t.country) AS country,
    -- select all other columns from 'temp_cleaned_globalweather'
    t.location_name,
    t.latitude,
    t.longitude,
    t.last_updated,
    t.temperature_celsius,
    t.wind_kph,
    t.pressure_mb,
    t.precip_mm,
    t.humidity,
    t.air_quality_Carbon_Monoxide,
    t.air_quality_Ozone,
    t.air_quality_Nitrogen_dioxide,
    t.air_quality_Sulphur_dioxide,
    t.`air_quality_PM2.5`,
    t.air_quality_PM10,
    t.`air_quality_us-epa-index`,
    t.`air_quality_gb-defra-index`
FROM temp_cleaned_globalweather t
LEFT JOIN flag_temp_country_names f
ON t.country = f.country;

DROP TEMPORARY TABLE flag_temp_country_names;
DROP TEMPORARY TABLE temp_cleaned_globalweather;


/* We can see there can be multiple locations by country
like in USA we have New York, Washington Harbor, Washington Park, etc
Also some country names still requiere cleaning like USA
Anyway, as we said before, the natural key is the location_name
*/
SELECT 
    country,
    location_name,
    COUNT(DISTINCT last_updated) AS number_of_dates_measured
FROM cleaned_globalweather
GROUP BY country, location_name
ORDER BY country;

-- -------------------------------------------------------------------------------
-- 			FEATURE ENGINEERING & ANALYSIS
-- -------------------------------------------------------------------------------

-- we build a function to determine the day segment
DELIMITER //
CREATE FUNCTION func_day_moment(hour_of_day INT)
RETURNS VARCHAR(10)
DETERMINISTIC
BEGIN
	RETURN CASE
		WHEN hour_of_day BETWEEN 0 AND 5 THEN 'Midnight'
		WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Dawn'
		WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Noon'
		ELSE 'Dusk'
	END;
END //
DELIMITER ;

-- we build a function to determine the season
DELIMITER //
CREATE FUNCTION func_season(lat FLOAT, month INT, day INT)
RETURNS VARCHAR(10)
DETERMINISTIC
BEGIN
    RETURN CASE
        -- Northern Hemisphere
        WHEN lat >= 0 THEN
            CASE
                WHEN (month = 12 AND day >= 21) OR month IN (1, 2) OR (month = 3 AND day < 21) THEN 'Winter'
                WHEN (month = 3 AND day >= 21) OR month IN (4, 5) OR (month = 6 AND day < 21) THEN 'Spring'
                WHEN (month = 6 AND day >= 21) OR month IN (7, 8) OR (month = 9 AND day < 21) THEN 'Summer'
                ELSE 'Fall'
            END
        -- Southern Hemisphere
        ELSE
            CASE
                WHEN (month = 12 AND day >= 21) OR month IN (1, 2) OR (month = 3 AND day < 21) THEN 'Summer'
                WHEN (month = 3 AND day >= 21) OR month IN (4, 5) OR (month = 6 AND day < 21) THEN 'Fall'
                WHEN (month = 6 AND day >= 21) OR month IN (7, 8) OR (month = 9 AND day < 21) THEN 'Winter'
                ELSE 'Spring'
            END
    END;
END //
DELIMITER ;


-- we create the first general view
/* For this view we take only locations with more than 40 measurements
In this view we find the moment of day, season of the measurements.
Also we calculate the accumulated rain per week.
*/
CREATE OR REPLACE VIEW vw_weather AS
SELECT
    cg.*,
    func_day_moment(HOUR(cg.last_updated)) AS day_segment,
    func_season(cg.latitude, MONTH(cg.last_updated), DAY(cg.last_updated)) AS season,
    ROUND(SUM(cg.precip_mm) OVER (
		PARTITION BY
			cg.country,
            cg.location_name,
            YEAR(cg.last_updated),
            WEEK(cg.last_updated)
		ORDER BY
			cg.last_updated
		), 2) AS weekly_accumulated_rain
FROM cleaned_globalweather cg
INNER JOIN (
    SELECT country, location_name
    FROM cleaned_globalweather
    GROUP BY country, location_name
    HAVING COUNT(DISTINCT last_updated) > 40
) filtered ON cg.country = filtered.country AND cg.location_name = filtered.location_name;


/* DISCLAIMER
The granularity of the dataset's dimensions is optimal, but regarding the measurements,
it is very poor. For relevant climatic data, we need consistent measurements. For a given
location, we need measurements throughout the day, or, in the worst-case scenario, at the
same hour each day. Data like temperature can vary widely throughout the day.

Ideally, we would take at least one location hourly along at least a year to make
a better study. But this dataset extends for less than three months and very disperselly.

Having said that, we clarify that the following measurements are not trustworthy for
real statistics. They are only provided as general examples of climatic statistics.
*/

-- finally we create a weekly view
CREATE OR REPLACE VIEW vw_weather_weekly AS
SELECT
    cg.country,
    cg.location_name,
    cg.latitude,
    cg.longitude,
    DATE(MIN(cg.last_updated)) AS week_start,
    ROUND(MAX(cg.temperature_celsius), 0) AS max_temperature_celsius,
    ROUND(MIN(cg.temperature_celsius), 0) AS min_temperature_celsius,
    ROUND(AVG(cg.wind_kph), 1) AS avg_wind_kph,
    ROUND(AVG(cg.pressure_mb), 0) AS avg_pressure_mb,
    ROUND(SUM(cg.precip_mm), 1) AS total_precip_mm,
    ROUND(AVG(cg.humidity), 1) AS avg_humidity,
    ROUND(AVG(cg.air_quality_Carbon_Monoxide), 1) AS avg_air_quality_Carbon_Monoxide,
    ROUND(AVG(cg.air_quality_Ozone), 1) AS avg_air_quality_Ozone,
    ROUND(AVG(cg.air_quality_Nitrogen_dioxide), 1) AS avg_air_quality_Nitrogen_dioxide,
    ROUND(AVG(cg.air_quality_Sulphur_dioxide), 1) AS avg_air_quality_Sulphur_dioxide,
    ROUND(AVG(cg.`air_quality_PM2.5`), 1) AS `avg_air_quality_PM2.5`,  -- keep original name for consistency
    ROUND(AVG(cg.air_quality_PM10), 1) AS avg_air_quality_PM10,  -- keep original name for consistency
    ROUND(AVG(cg.`air_quality_us-epa-index`), 1) AS `avg_air_quality_us-epa-index`,  -- keep original name for consistency
    ROUND(AVG(cg.`air_quality_gb-defra-index`), 1) AS `avg_air_quality_gb-defra-index`  -- keep original name for consistency
FROM
    cleaned_globalweather cg
INNER JOIN (
    SELECT country, location_name
    FROM cleaned_globalweather
    GROUP BY country, location_name
    HAVING COUNT(DISTINCT last_updated) > 40
) filtered ON cg.country = filtered.country AND cg.location_name = filtered.location_name
GROUP BY
    cg.country,
    cg.location_name,
    cg.latitude,
    cg.longitude,
    YEAR(cg.last_updated),
    WEEK(cg.last_updated);

SELECT * FROM vw_weather_weekly
ORDER BY 1, 2, 5;

SELECT 
    country, 
    location_name, 
    COUNT(*) AS total_weekly_measurements
FROM 
    vw_weather_weekly
GROUP BY 
    country, 
    location_name
ORDER BY
	country, 
    location_name;