# DataPipeline

## Purpose of project
주식 데이터 분석 및 시계열 모델을 위한 DW 구축. 이를 위한 배치 파이프라인 구축 프로젝트를 목표.\
파이프라인은 Google Cloud Platform위에서 Batch로 Incremental하게 동작합니다.

## Process
1. Daily 스케쥴로 주식 데이터를 API를 활용해 로컬(Cloud Compute Engine)로 가져옵니다.
2. 가져온 데이터를 Cloud Dataproc(Spark/Hadoop)를 활용해 Transform을 수행합니다.
3. Transformed 된 결과를 GCS(Cloud Storage)에 저장한 후 BigQuery에 업데이트 합니다.\
3-1. 이때 파이프라인은 백필등의 작업에도 멱등성을 보장합니다.

## Architecture of Data Pipeline
Perspectives of movement of data
![Architecture of Pipeline](./images/Pipeline_Architecture.png)

## Considerations
### IAM
1. Compute Engine에서 Google Cloud Storage내 리소스를 조회하고, 변경, 삭제 -> 개체 관리자
2. Compute Engine에서 BigQuery project 내 테이블 생성, 삭제, 변경. -> BigQuery 관리자\
![Variables](./images/iam_roles.png)

### Service Account
[서비스 계정이란](https://www.notion.so/IAM-21def2538f424a89a173a6ea3abbb3ae?pvs=4#7ed255a943ee458bb0027123d9fa86bd)\
Compute Engine에서 Dataproc 등 다른 서비스를 API로 이용할 때 인증된 계정을 사용.\
Compute Engine에 대해 아래와 같은 서비스 계정을 생성해 API를 수행. 
![Service account for API](./images/ServiceAccount.png)

### Others for Security
API Key 및 작업 환경 경로 등을 위해서 코드에 직접적으로 작성하기 보다 Variables나 Connections를 만들어 사용함.
1. Variables\
![Variables](./images/Variables.png)

2. Connections\
![Connections](./images/Connections.png)

### Cost
Dataproc 비용은 초 단위로 청구. 따라서 클러스터를 생성해 지속사용하는 것은 비용 낭비라고 생각함.\
위 파이프라인은 하루에 한 번 돌아가는 배치 형태이므로 매 DAG 실행마다 클러스터를 생성하고 삭제하는 형식으로 파이프라인이 동작.

### Schema
1. csv와 BigQuery간 데이터 타입 문제.\
csv는 컬럼 별 형식을 가지지 않기 때문에 string 형태와 data format 형태가 달라 문제 발생.\
이를 해결하기 위해 Json 형태의 Fixed된 스키마 형태를 사용했고 Date, Datetime, Time, Timestamp의 차이를 배웠음. (Refer/Schema에 정리)

2. BigQuery Performance 문제.\
하루에 약 300Kb가량의 적은 데이터이지만 매일 쌓이기 때문에 장기간 사용시 테이블의 크기가 매우 커질 수 있음.\
Clustering과 Partitioning을 활용해 해결. [Clustering과 Partitioning이란](https://www.notion.so/NoSQL-c714213918d84e17b0896f6d62b390d7?pvs=4#59d52765cd3c4f8dbcb1aab24468617f)

### Idempotency(멱등성)
Incremental Update(Daily)이므로 멱등성 보장이 필요.\
DW는 PK개념이 없기 때문에 다음과 같이 멱등성을 구현.
1. 기존 Origin 테이블을 tmp 테이블로 복사(Create Table As Select)
2. 새로운 데이터를 tmp 테이블로 업로드.
3. Window 함수를 사용해 가장 최근 업데이트 된 데이터만 추출해 원본 테이블을 업데이트.

## Final DAG
![DAG](./images/Dag_flow.png)

## BigQuery Partitioning & Clustering
Partitioning : 날짜 별로 파티셔닝을 하여 Incremental Update 시 데이터가 계속해서 증가해도 Performance를 보장.\
Clustering : 종목(KOSPI, KODAQ 등) 또는 기업 별 데이터 분석 및 시계열 모델 생성을 위해 데이터 추출에 있어 빠른 성능을 보장하기 위해 해당 필드를 기준으로 클러스터링 생성

![DAG](./images/Bigquery_schema.png)

## 추후 버전 업그레이드 예정\
문제 1. DAG의 수가 급격히 늘어나서 Compute Engine의 부하가 커진다면?\
- &#x2610; GKE(google kubenetes engine)활용 (4월)\
- &#x2610; 비용, 트래픽 관점에서 모니터링, 오토 스케일링(CA & HPA) 사용방안 고민(4월)


문제 2. 조직이 커지면서 표준화된 규칙이 필요해진다면? 어떤 정책이 필요할지.

1) 코드 관점\
   i. Base된 Operator를 생성. 반드시 필요한 부분과 Optional한 부분을 명확히 정의.
   
2) DW 관점\
   i. 테스트와 운영을 어떻게 분리? -> test, tmp 등의 하드 코딩으로 사용하지 않는다.
   
문제 3. DAG 끼리의 의존성 문제가 복잡해진다면? 어떤 정책이 필요한지.\
   i. 최종 Needs만을 중심으로 개발하는 게 아닌 표준화된 프로세스에 근거해 파이프라인 작성이 필요함.

- &#x2610; 채권 등 다른 데이터 파이프라인 개발(5월)
- &#x2610; ML Pipeline 개발(LSTM) (5월)
- &#x2610; 스트리밍 파이프라인(Pub/Sub Dataflow BigTable) (미정)

&#x2610; &#x2611;
