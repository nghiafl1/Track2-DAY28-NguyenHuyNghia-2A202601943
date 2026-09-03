# Báo cáo Tổng kết - Day 28 (Track 2)

## 1. Sơ đồ Kiến trúc & Phân quyền (Architecture / Ownership)

```mermaid
flowchart TD
    %% Users & External
    Client([Client / Postman])
    
    %% Gateway
    Gateway(API Gateway - Envoy)
    
    %% API Services (Team Serving)
    API(FastAPI Orchestrator)
    vLLM(vLLM Server - GPU)
    
    %% ML & Data (Team Data)
    Feast(Feast Online Store)
    Qdrant(Qdrant Vector DB)
    MLflow(MLflow Registry)
    Delta[(Delta Lake)]
    Spark(Spark Connect)
    
    %% Ingestion (Team Ingestion)
    Kafka(Kafka Event Bus)
    Airflow(Airflow DAG)
    
    %% Telemetry (Team Platform)
    OTEL(OTEL Collector)
    Prometheus(Prometheus)
    Grafana(Grafana)
    Jaeger(Jaeger)

    %% Flow
    Client -->|HTTP/REST| Gateway
    Gateway -->|Rate Limited / Routed| API
    
    API -->|Ingest Feedback/Docs| Kafka
    Kafka -->|Consume (Replay)| Airflow
    Airflow -->|Trigger| Spark
    Spark -->|MERGE| Delta
    Delta -->|Materialize| Feast
    Delta -->|Embeddings| Qdrant
    
    API -->|Get Online Features| Feast
    API -->|Retrieve Grounding| Qdrant
    API -->|Resolve Champion| MLflow
    API -->|Chat Completion| vLLM
    
    %% Telemetry Links
    Gateway -.-> OTEL
    API -.-> OTEL
    Kafka -.-> OTEL
    Airflow -.-> OTEL
    Spark -.-> OTEL
    OTEL -.->|Metrics| Prometheus
    OTEL -.->|Traces| Jaeger
    Prometheus -.-> Grafana

    %% Styles
    classDef platform fill:#f9f,stroke:#333,stroke-width:2px;
    classDef ingestion fill:#bbf,stroke:#333,stroke-width:2px;
    classDef data fill:#bfb,stroke:#333,stroke-width:2px;
    classDef serving fill:#fbb,stroke:#333,stroke-width:2px;
    
    class Gateway,OTEL,Prometheus,Grafana,Jaeger platform;
    class Kafka,Airflow ingestion;
    class Delta,Spark,Feast,MLflow data;
    class API,vLLM,Qdrant serving;
```

## 2. Phân tích Minh chứng Tải & Nút thắt cổ chai (Load Profile & Bottleneck)

Dưới đây là kết quả từ kịch bản Test Load (`load-tests/run_profile.py`) với **100 requests** và **8 workers**:

```json
{
  "requests": 100,
  "workers": 8,
  "status_counts": {
    "200": 27,
    "0": 73
  },
  "latency_ms": {
    "p50": 11.419,
    "p95": 4899.362,
    "p99": 4949.741
  }
}
```

### Phân tích:
- **Tỉ lệ thành công:** Chỉ có `27/100` requests thành công, `73/100` requests bị từ chối (Status Code 0 đại diện cho HTTP Error bắt được do Rate Limit Timeout hoặc Connection Error tại Python client).
- **Nguyên nhân (Bottleneck):** Nút thắt cổ chai ở đây là **Cơ chế Rate Limit của Envoy Gateway (IP08)**. Gateway đang được cấu hình `max_tokens: 10`, `fill_interval: 1s` (10 RPS). Khi 8 workers bắn đồng loạt 100 requests, Gateway lập tức cắt (refuse) phần dư thừa để bảo vệ Downstream (API).
- **Đánh giá Latency:** 
  - `P50 = 11.4 ms`: Cực kỳ nhanh vì 73% requests bị từ chối ở Edge (Gateway) nên không tốn thời gian xử lý tại backend.
  - `P95 ~ 4.8s / P99 ~ 4.9s`: Các request lọt vào được queue của ứng dụng phải đợi tài nguyên từ vLLM hoặc Qdrant, cộng thêm timeout của httpx client chờ nhận token (Nếu có Retry). Nhìn chung, hệ thống phục vụ an toàn theo giới hạn thiết kế mà không bị Crash (Fail-fast).

## 3. Luồng Lỗi & Khôi phục (Failure/Recovery & No Data Loss)
- Hệ thống áp dụng nguyên tắc **Degraded Response (Fail-open cho Serving)**. Nếu Feast Store bị mất kết nối, hệ thống vẫn phục vụ câu trả lời thay vì sập (chỉ ghi nhận `degraded = true`).
- Nguyên tắc **Zero Data Loss (Fail-safe cho Ingestion)**: Mọi thao tác submit data đều được lưu ngay vào Kafka (`data.raw`). Dù Airflow hoặc Spark sập, offset trong Kafka không bị mất. Khi khôi phục, quá trình xử lý sẽ bắt đầu lại từ offset chưa commit (Idempotent MERGE với Delta Lake chống trùng lặp dữ liệu).

## 4. Những Giới Hạn & Rủi ro khi lên Production (Production Gaps)
Môi trường hiện tại được tối ưu cho Lab/Education, khi mang lên Production (AWS/GCP), các rủi ro sau cần giải quyết:
1. **Single Point of Failure ở Message Broker:** Kafka đang chạy với 1 Broker, `replication_factor: 1`. Cần triển khai Kafka Cluster (MSK/Confluent) tối thiểu 3 nodes.
2. **Lưu trữ Cục bộ (Local Storage):** MLflow dùng SQLite, Qdrant/Delta lưu data ở volume `/workspace`. Production phải sử dụng S3/GCS Object Storage và PostgreSQL (RDS) làm Backend Store.
3. **Cold Start & Dynamic Scaling:** Chưa có cơ chế Scale Out tự động (HPA) cho LLM/API. Inference Endpoint (GPU) đang hardcode tĩnh 1 replica.
4. **Bảo mật & Phân quyền:** Chưa có xác thực Authentication (OAuth2/JWT) ở API Gateway. Hiện tại route `/api/v1/ask` đang public 100%.

## 5. Thống kê Đóng góp (Contributions)
- **Nguyen Huy Nghia (Platform Engineer / Operator):** Hoàn thành tích hợp 10 IP, xử lý lỗi Telemetry, fix lỗi Kafka Topic/Readiness Probe, hoàn thiện hệ thống đo lường OTLP và triển khai thành công Evidence Collection. Đóng vai trò Incident Commander giải quyết tắc nghẽn luồng tích hợp hệ thống.
