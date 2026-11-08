# Architecture and evidence boundary

```mermaid
flowchart LR
  Client[Authenticated event client] --> Ingest[Go ingest]
  Ingest -->|all successful broker callbacks before 202| Kafka[Kafka events: 12 partitions]
  Kafka --> Consumer[Bounded partition consumers]
  Consumer -->|confirmed inserts before offsets| Raw[ClickHouse raw facts]
  Raw --> Identity[Global min/max normalized identity states]
  Identity -->|same-payload identities only| Refresh[Atomic full-history refreshes]
  Refresh --> Metrics[Canonical minute snapshots]
  Metrics --> Query[Tenant-scoped query API]
  Consumer -->|permanent poison| DLQ[Kafka DLQ]
  DLQ --> Replay[Bounded audited replay CLI]
  Replay -->|original logical ID| Kafka
  Consumer --> Audit[PostgreSQL compact DLQ audit]
  Replay --> Audit
  Refresh -. measured memory/readiness bottleneck .-> Failed[Query/freshness/metric gates failed at scale]
```

```mermaid
flowchart TB
  Reviewer[Separate plan and spend approval] --> TF[Reviewed Terraform / saved plan]
  GH[Protected manual GitHub environment] -->|OIDC, ECR publication only| ECR[Private immutable image digests]
  Client[Optional approved client CIDRs] --> ALB[Optional HTTPS ALB / ACM]
  subgraph PrivateVPC[Prepared private dev VPC — single AZ runtime]
    ALB --> Ingest[ECS ingest]
    ALB --> Query[ECS query]
    Ingest -->|TLS + SCRAM| Broker[EC2 Kafka + TLS PostgreSQL]
    Broker --> Consume[ECS consumer]
    Consume -->|verified HTTPS| CH[EC2 ClickHouse]
    Query -->|verified HTTPS| CH
    Consume -->|verify-full TLS| Broker
    SSM[SSM operator access — no SSH] --> Broker
    Secrets[Per-service Secrets Manager entries] --> Ingest
    Secrets --> Consume
    Secrets --> Query
    Endpoints[Private AWS endpoints + S3 gateway] --> ECR
    Broker --> Vol1[Retained encrypted gp3]
    CH --> Vol2[Retained encrypted gp3]
  end
  TF --> PrivateVPC
```

Local pipeline/fault evidence is verified. The AWS drawing represents executable **offline-validated preparation**, not deployed resources. Two public subnets are created only for the opt-in ALB; database/broker ingress remains private. External HTTPS terminates at the ALB; the private target hop is HTTP. Root/server secrets do not enter Terraform state. The shared Kafka/PostgreSQL node is one administrative/failure boundary; no HA, automatic credential rotation or 24x7 operator guarantee is claimed. Cloud performance, negative IAM/SG checks and cleanup inventory remain unverified until an approved environment exists.
