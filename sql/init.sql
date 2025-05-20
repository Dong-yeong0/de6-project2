-- Create database and schemas
CREATE DATABASE PUBLIC_TRANSPORTATION;

CREATE SCHEMA ADHOC;
CREATE SCHEMA ANALYTICS;
CREATE SCHEMA RAW_DATA;


-- ============================================================================
-- RAW DATA TABLES
-- ============================================================================

-- Subway Usage Table
CREATE OR REPLACE TABLE RAW_DATA.SUBWAY_USAGE (
    LINE_NUMBER        VARCHAR(16777216) COMMENT '라인 번호',
    STATION_NAME       VARCHAR(16777216) COMMENT '역 이름',
    BOARDING_COUNT     NUMBER(38, 0)     COMMENT '탑승 인원',
    ALIGHTING_COUNT    NUMBER(38, 0)     COMMENT '하차 인원',
    USAGE_DATE         DATE              COMMENT '사용 일자',
    REGISTER_DATE      DATE              COMMENT '등록 일자'
);

-- Bus Usage Table
CREATE OR REPLACE TABLE RAW_DATA.BUS_USAGE (
    USAGE_DATE         DATE              COMMENT '버스 이용 일자 (YYYY-MM-DD 형식)',
    ROUTE_NUMBER       VARCHAR(16777216) COMMENT '버스 노선 번호 (예: 7726, 100 등)',
    ROUTE_NAME         VARCHAR(16777216) COMMENT '노선 이름 또는 버스 노선 구분 (예: 간선, 지선 등)',
    NODE_ID            VARCHAR(16777216) COMMENT '표준 버스 정류장 ID (고유 식별자)',
    ARS_ID             VARCHAR(16777216) COMMENT 'ARS 버스정류장 번호 (정류장 고유번호)',
    STATION_NAME       VARCHAR(16777216) COMMENT '버스 정류장 이름',
    BOARDING_COUNT     NUMBER(38, 0)     COMMENT '해당 정류장의 승차 총 승객 수',
    ALIGHTING_COUNT    NUMBER(38, 0)     COMMENT '해당 정류장의 하차 총 승객 수',
    REGISTER_DATE      DATE              COMMENT '데이터 등록 일자 (YYYYMMDD 형식)'
);

-- Bike Rental Usage Table
CREATE OR REPLACE TABLE RAW_DATA.BIKE_RENTAL_USAGE (
    USAGE_DATE           DATE              COMMENT '데이터 기준 날짜 (YYYY-MM-DD HH:MM:SS)',
    AGGREGATION_UNIT     VARCHAR(16777216) COMMENT '집계 기준 (예: 일간, 주간, 월간 등)',
    START_SPOT_ID        VARCHAR(16777216) COMMENT '대여 시작 대여소 ID',
    START_SPOT_NAME      VARCHAR(16777216) COMMENT '대여 시작 대여소 이름',
    END_SPOT_ID          VARCHAR(16777216) COMMENT '반납 종료 대여소 ID',
    END_SPOT_NAME        VARCHAR(16777216) COMMENT '반납 종료 대여소 이름',
    TOTAL_RENTALS        NUMBER(38, 0)     COMMENT '해당 구간의 전체 대여 건수',
    TOTAL_USAGE_MINUTES  NUMBER(38, 0)     COMMENT '총 이용 시간 (분)',
    TOTAL_USAGE_DISTANCE FLOAT             COMMENT '총 이용 거리 (Km 단위)'
);


-- ============================================================================
-- ANALYTICS TABLES
-- ============================================================================

-- FACT Table: Usage
CREATE OR REPLACE TABLE ANALYTICS.FACT_USAGE (
    RECORD_ID           NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    DATE_ID             DATE             NOT NULL,
    STATION_ID          VARCHAR(50)      NOT NULL,
    TRANSPORT_TYPE_ID   NUMBER           NOT NULL,
    HOUR                NUMBER(2, 0)     NOT NULL,
    BOARDING_COUNT      NUMBER DEFAULT 0,
    GETOFF_COUNT        NUMBER DEFAULT 0,

    CONSTRAINT FK_FACT_USAGE_DATE 
        FOREIGN KEY (DATE_ID) REFERENCES ANALYTICS.DIM_DATE(DATE_ID),
    CONSTRAINT FK_FACT_USAGE_STATION 
        FOREIGN KEY (STATION_ID) REFERENCES ANALYTICS.DIM_STATION(STATION_ID),
    CONSTRAINT FK_FACT_USAGE_TRANSPORT_TYPE 
        FOREIGN KEY (TRANSPORT_TYPE_ID) REFERENCES ANALYTICS.DIM_TRANSPORT_TYPE(TRANSPORT_TYPE_ID),

    CLUSTER BY (DATE_ID)
);

-- DIM Table: Date
CREATE OR REPLACE TABLE ANALYTICS.DIM_DATE (
    DATE_ID         DATE          PRIMARY KEY,
    YEAR            NUMBER(4, 0)  NOT NULL,
    MONTH           NUMBER(2, 0)  NOT NULL,
    DAY_OF_MONTH    NUMBER(2, 0)  NOT NULL,
    DAY_OF_WEEK_KR  VARCHAR(10)   NOT NULL,
    IS_WEEKEND      BOOLEAN       NOT NULL,
    QUARTER         NUMBER(1, 0)  NOT NULL,
    YEAR_MONTH      VARCHAR(7)    NOT NULL
);

-- DIM Table: Station
CREATE OR REPLACE TABLE ANALYTICS.DIM_STATION (
    STATION_ID      VARCHAR(50)   PRIMARY KEY COMMENT '역 ID',
    STATION_NAME    VARCHAR(255)  NOT NULL    COMMENT '역 이름',
    STATION_TYPE    VARCHAR(20)   NOT NULL    COMMENT '정류소 종류',
    ADDRESS         VARCHAR(255)              COMMENT '주소',
    CITY            VARCHAR(50)               COMMENT '도시',
    LATITUDE        DECIMAL(9, 6)             COMMENT '위도',
    LONGITUDE       DECIMAL(9, 6)             COMMENT '경도'
);

-- DIM Table: Transport Type
CREATE OR REPLACE TABLE ANALYTICS.DIM_TRANSPORT_TYPE (
    TRANSPORT_TYPE_ID  NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    MAIN_CATEGORY      VARCHAR(20)   NOT NULL,
    SUB_CATEGORY       VARCHAR(50)   NOT NULL
);
