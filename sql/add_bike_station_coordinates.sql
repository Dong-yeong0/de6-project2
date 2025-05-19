-- 5.19 mon 따릉이 위치

-- raw_data 스키마에 csv를 가져올 테이블 생성
CREATE TABLE PUBLIC_TRANSPORTATION.RAW_DATA.BIKE_STATION_COORDINATES (
    station_id   VARCHAR(50)    NOT NULL    PRIMARY KEY COMMENT '대여소_ID',
    address1     VARCHAR(255)   NOT NULL                COMMENT '주소1',
    address2     VARCHAR(255)              NULL          COMMENT '주소2',
    latitude     DECIMAL(9,6)   NOT NULL                COMMENT '위도',
    longitude    DECIMAL(9,6)   NOT NULL                COMMENT '경도'
);

-- stage를 사용해서 벌크 업데이트
COPY INTO public_transportation.raw_data.BIKE_STATION_COORDINATES
FROM @public_transportation.raw_data.dawit_s3/BIKE_STATION_COORDINATES.csv;

select * from public_transportation.raw_data.bike_station_coordinates;

select * from public_transportation.analytics.dim_station
    where station_type = '자전거대여소';

SELECT *
FROM PUBLIC_TRANSPORTATION.RAW_DATA.bike_station_coordinates
WHERE latitude = 0
  AND longitude = 0;

DELETE FROM PUBLIC_TRANSPORTATION.RAW_DATA.bike_station_coordinates
WHERE latitude = 0
  AND longitude = 0;

-- dim_station 에 위도/경도 업데이트
UPDATE analytics.dim_station AS d
SET
  d.latitude  = c.latitude,
  d.longitude = c.longitude,
  d.address   = c.address1 || ' ' || c.address2
FROM public_transportation.raw_data.bike_station_coordinates AS c
WHERE d.station_id = c.station_id
  AND (d.latitude IS NULL OR d.longitude IS NULL);
