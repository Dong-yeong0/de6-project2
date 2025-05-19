-- 25.5.19 mon 지하철 위치 정보 가져오기

-- Seoul_Metro_Lines_1-8_Station_Coordinates_Latitude_Longitude.csv 가져올 테이블 생성
CREATE TABLE seoul_metro_station_coordinates (
    seq_no                    INT             PRIMARY KEY,          -- 연번
    line_no                   INT             NOT NULL,             -- 호선
    external_station_code     INT             NOT NULL,             -- 고유역번호(외부역코드)
    station_name              VARCHAR(50)     NOT NULL,             -- 역명
    latitude                  DECIMAL(8,6)    NOT NULL,             -- 위도
    longitude                 DECIMAL(9,6)    NOT NULL,             -- 경도
    created_date              DATE            NOT NULL              -- 작성일자
);

-- 스테이지 생성
CREATE OR REPLACE STAGE public_transportation.raw_data.dawit_s3
  URL='s3://dawit-test01-bucket/test_data'
  CREDENTIALS=(
    AWS_KEY_ID=[YOUR_AWS_KEY]
    AWS_SECRET_KEY=[YOUR_SECRET_KEY]
  )
  FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- 벌크 업데이트
COPY INTO public_transportation.raw_data.seoul_metro_station_coordinates
FROM @public_transportation.raw_data.subway_station_stage/Seoul_Metro_Lines_1-8_Station_Coordinates_Latitude_Longitude.csv;

select distinct station_id from public_transportation.analytics.fact_usage;

select * from public_transportation.analytics.dim_station;


select * from public_transportation.analytics.dim_station
    where station_type = '지하철역';

UPDATE analytics.dim_station AS d
SET
  d.latitude  = c.latitude,
  d.longitude = c.longitude
FROM public_transportation.raw_data.seoul_metro_station_coordinates AS c
WHERE d.station_id = 'SUB_'||c.station_name
  AND (d.latitude  IS NULL OR d.longitude IS NULL);


select * from public_transportation.analytics.dim_station
    where station_type = '자전거대여소';

select distinct station_type from public_transportation.analytics.dim_station;
