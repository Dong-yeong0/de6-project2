# de6-project2
프로젝트에서 사용된 파이썬 스트립트, SQL을 관리하는 프로젝트입니다.

## 📂 ETL 과정

```mermaid
flowchart TD
  A[OpenAPI & 월별 Sheet 파일] --> B["Python 스크립트<br/>(requests + pandas)"]
  B --> C[Amazon S3 스테이징]
  C --> D["Snowflake DW<br/>(Star Schema)"]
  D --> E[Apache Superset 대시보드]

