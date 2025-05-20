INSERT INTO analytics.dim_date (date_id, year, month, day_of_month, day_of_week_kr, is_weekend, quarter, year_month)
WITH all_dates AS (
    SELECT DISTINCT
        TO_DATE(raw.operation_date, 'YYYYMMDD') AS clean_date
    FROM raw_data.subway_hourly_usage raw
    WHERE raw.operation_date IS NOT NULL
      AND TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL
)
SELECT
    ad.clean_date AS date_id,
    YEAR(ad.clean_date) AS year,
    MONTH(ad.clean_date) AS month,
    DAYOFMONTH(ad.clean_date) AS day_of_month,
    CASE DAYOFWEEKISO(ad.clean_date)
        WHEN 1 THEN '월' WHEN 2 THEN '화' WHEN 3 THEN '수' WHEN 4 THEN '목'
        WHEN 5 THEN '금' WHEN 6 THEN '토' WHEN 7 THEN '일'
    END AS day_of_week_kr,
    IFF(DAYOFWEEKISO(ad.clean_date) IN (6, 7), TRUE, FALSE) AS is_weekend,
    QUARTER(ad.clean_date) AS quarter,
    TO_VARCHAR(ad.clean_date, 'YYYY-MM') AS year_month
FROM all_dates ad
WHERE NOT EXISTS (SELECT 1 FROM analytics.dim_date existing WHERE existing.date_id = ad.clean_date);

INSERT INTO analytics.dim_transport_type (main_category, sub_category)
WITH transport_types_source AS (
    SELECT DISTINCT
        '지하철' AS main_category,
        raw.line_name AS sub_category
    FROM raw_data.subway_hourly_usage raw 
    WHERE raw.line_name IS NOT NULL AND raw.line_name != ''
)
SELECT
    tts.main_category,
    tts.sub_category
FROM transport_types_source tts
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.dim_transport_type existing
    WHERE existing.main_category = tts.main_category AND existing.sub_category = tts.sub_category
);

INSERT INTO analytics.dim_station (station_id, station_name, station_type, address, city, longitude, latitude)
WITH station_details_source AS (
    SELECT DISTINCT
        'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION') AS station_id,
        COALESCE(raw.station_name, 'Unknown Subway Station') AS station_name,
        '지하철역' AS station_type,
        NULL AS address, NULL AS city, NULL AS longitude, NULL AS latitude
    FROM raw_data.subway_hourly_usage raw 
    WHERE raw.station_name IS NOT NULL AND raw.station_name != ''
)
SELECT
    sds.station_id,
    sds.station_name,
    sds.station_type,
    sds.address,
    sds.city,
    sds.longitude,
    sds.latitude
FROM station_details_source sds
WHERE sds.station_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM analytics.dim_station existing WHERE existing.station_id = sds.station_id);


INSERT INTO analytics.fact_usage (date_id, station_id, transport_type_id, hour, boarding_count, getoff_count)

-- 시간대 00-01
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    0 AS hour,
    COALESCE(raw.boarding_00_01, 0) AS boarding_count,
    COALESCE(raw.getoff_00_01, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 01-02
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    1 AS hour,
    COALESCE(raw.boarding_01_02, 0) AS boarding_count,
    COALESCE(raw.getoff_01_02, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 02-03
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    2 AS hour,
    COALESCE(raw.boarding_02_03, 0) AS boarding_count,
    COALESCE(raw.getoff_02_03, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 03-04
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    3 AS hour,
    COALESCE(raw.boarding_03_04, 0) AS boarding_count,
    COALESCE(raw.getoff_03_04, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 04-05
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    4 AS hour,
    COALESCE(raw.boarding_04_05, 0) AS boarding_count,
    COALESCE(raw.getoff_04_05, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 05-06
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    5 AS hour,
    COALESCE(raw.boarding_05_06, 0) AS boarding_count,
    COALESCE(raw.getoff_05_06, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 06-07
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    6 AS hour,
    COALESCE(raw.boarding_06_07, 0) AS boarding_count,
    COALESCE(raw.getoff_06_07, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 07-08
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    7 AS hour,
    COALESCE(raw.boarding_07_08, 0) AS boarding_count,
    COALESCE(raw.getoff_07_08, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 08-09
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    8 AS hour,
    COALESCE(raw.boarding_08_09, 0) AS boarding_count,
    COALESCE(raw.getoff_08_09, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 09-10
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    9 AS hour,
    COALESCE(raw.boarding_09_10, 0) AS boarding_count,
    COALESCE(raw.getoff_09_10, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 10-11
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    10 AS hour,
    COALESCE(raw.boarding_10_11, 0) AS boarding_count,
    COALESCE(raw.getoff_10_11, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 11-12
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    11 AS hour,
    COALESCE(raw.boarding_11_12, 0) AS boarding_count,
    COALESCE(raw.getoff_11_12, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 12-13
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    12 AS hour,
    COALESCE(raw.boarding_12_13, 0) AS boarding_count,
    COALESCE(raw.getoff_12_13, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 13-14
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    13 AS hour,
    COALESCE(raw.boarding_13_14, 0) AS boarding_count,
    COALESCE(raw.getoff_13_14, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 14-15
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    14 AS hour,
    COALESCE(raw.boarding_14_15, 0) AS boarding_count,
    COALESCE(raw.getoff_14_15, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 15-16
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    15 AS hour,
    COALESCE(raw.boarding_15_16, 0) AS boarding_count,
    COALESCE(raw.getoff_15_16, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 16-17
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    16 AS hour,
    COALESCE(raw.boarding_16_17, 0) AS boarding_count,
    COALESCE(raw.getoff_16_17, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 17-18
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    17 AS hour,
    COALESCE(raw.boarding_17_18, 0) AS boarding_count,
    COALESCE(raw.getoff_17_18, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 18-19
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    18 AS hour,
    COALESCE(raw.boarding_18_19, 0) AS boarding_count,
    COALESCE(raw.getoff_18_19, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 19-20
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    19 AS hour,
    COALESCE(raw.boarding_19_20, 0) AS boarding_count,
    COALESCE(raw.getoff_19_20, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 20-21
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    20 AS hour,
    COALESCE(raw.boarding_20_21, 0) AS boarding_count,
    COALESCE(raw.getoff_20_21, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 21-22
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    21 AS hour,
    COALESCE(raw.boarding_21_22, 0) AS boarding_count,
    COALESCE(raw.getoff_21_22, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 22-23
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    22 AS hour,
    COALESCE(raw.boarding_22_23, 0) AS boarding_count,
    COALESCE(raw.getoff_22_23, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL

UNION ALL

-- 시간대 23-00 (또는 23-24)
SELECT
    TO_DATE(raw.operation_date, 'YYYYMMDD') AS date_id,
    ds.station_id,
    dtt.transport_type_id,
    23 AS hour,
    COALESCE(raw.boarding_23_00, 0) AS boarding_count,
    COALESCE(raw.getoff_23_00, 0) AS getoff_count
FROM raw_data.subway_hourly_usage raw
JOIN analytics.dim_station ds ON ds.station_id = 'SUB_' || COALESCE(raw.station_name, 'UNKNOWN_SUB_STATION')
JOIN analytics.dim_transport_type dtt ON dtt.main_category = '지하철' AND dtt.sub_category = raw.line_name
WHERE TRY_TO_DATE(raw.operation_date, 'YYYYMMDD') IS NOT NULL;