.timer on
.mode ascii
.echo on

-- Run calculations
WITH src AS (SELECT station_name,
                    MIN(measurement) AS min_measurement,
                    CAST(AVG(measurement) AS DECIMAL(8,1)) AS mean_measurement,
                    MAX(measurement) AS max_measurement
            FROM READ_PARQUET('measurements.parquet')
            GROUP BY station_name)
    SELECT '{' ||
            ARRAY_TO_STRING(LIST_SORT(LIST(station_name || '=' || CONCAT_WS('/',min_measurement, mean_measurement, max_measurement))),', ') ||
            '}' AS "1BRC"
    FROM src;

.quit
