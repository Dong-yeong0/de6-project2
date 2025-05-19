-- 5.19 Mon 버스 정류장

-- csv 데이터 가져올 테이블 생성
CREATE or replace TABLE PUBLIC_TRANSPORTATION.RAW_DATA.BUS_STOP_COORDINATES (
    NODE_ID       VARCHAR(50)    PRIMARY KEY COMMENT '정류소 고유번호 (NODE_ID)',
    ARS_ID        VARCHAR(50)     NOT NULL    COMMENT 'ARS ID',
    STOP_NAME_KR  VARCHAR(255)    NOT NULL    COMMENT '정류소명',
    X_COORD       DECIMAL(9,6)  NOT NULL    COMMENT 'X좌표',
    Y_COORD       DECIMAL(9,6)  NOT NULL    COMMENT 'Y좌표',
    STOP_TYPE     VARCHAR(50)                  COMMENT '정류소타입'
);

-- 스테이지 생성
CREATE OR REPLACE STAGE public_transportation.raw_data.dawit_s3
  URL='s3://dawit-test01-bucket/test_data'
  CREDENTIALS=(
    AWS_KEY_ID=[YOUR_AWS_KEY]
    AWS_SECRET_KEY=[YOUR_SECRET_KEY]
  )
  FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO public_transportation.raw_data.BUS_STOP_COORDINATES
FROM @public_transportation.raw_data.dawit_s3/BUS_STOP_COORDINATES.csv;

select * from public_transportation.analytics.dim_station
    where station_type = '버스';

select * from public_transportation.raw_data.bus_stop_coordinates;

UPDATE analytics.dim_station AS d
SET
  d.latitude  = c.y_coord,
  d.longitude = c.x_coord
FROM public_transportation.raw_data.bus_stop_coordinates AS c
WHERE d.station_id = c.node_id
  AND (d.latitude  IS NULL OR d.longitude IS NULL);

UPDATE analytics.dim_station AS d
SET
  d.latitude  = NULL,
  d.longitude = NULL
FROM public_transportation.raw_data.bus_stop_coordinates AS c
WHERE d.station_id = c.node_id;


select * from public_transportation.analytics.dim_station;
