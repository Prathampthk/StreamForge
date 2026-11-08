# Cloud cost review — no spend approved

Default region is us-east-2, but a live account/region and price quote have not been selected. These are **illustrative planning amounts**, not a current Ohio quote or a billing cap. They use public US East example rates and 730 hours/month; verify every regional line in AWS Pricing Calculator and review the authenticated resource plan before approval. No credits/free tier/discounts are assumed.

| Private dev component | Planning assumption | Approximate monthly amount |
|---|---|---:|
| Kafka/PostgreSQL host | one t3.medium at $0.0416/h | $30 |
| ClickHouse host | one r6i.large, illustrative $0.126/h | $92 |
| Three Fargate apps | total 1 vCPU +2GiB, $0.04048/vCPU-h +$0.004445/GiB-h | $36 |
| PrivateLink | six endpoints in one AZ at illustrative $0.01/endpoint-h | $44 |
| gp3 disks | 240GiB (two roots + two data disks), illustrative $0.08/GiB-month | $19 |
| Secrets/KMS/zones | six secrets, two customer keys, private DNS | roughly $5–8 |
| Logs, S3/ECR storage, requests, data processing | workload-dependent, excluded above | additional |

A continuously running private demo is roughly **$230–$300/month for low log/data volume**, not a free deployment. Two running hours have a much smaller compute fraction, but minimum billing, retained resources, logs/storage and requests still apply; use a separately approved dollar budget. No automated spend cap is implemented. ALB/LCU/public IPv4/public zone/certificate-domain costs are optional additions; no NAT or RDS is included. Container Insights is disabled to avoid implicit enhanced-observability charges; ordinary ECS metrics and scoped CloudWatch logs remain.

Stopping EC2/tasks does **not** remove endpoint, EBS, key, secret, zone or storage costs. The retained 240GiB disks plus six endpoints alone are about $63/month under these illustrative rates, before other retained resources. Approved cleanup/retention choices matter more than merely stopping processes. Kafka's T3 credit mode is standard, so sustained CPU can throttle rather than silently incur unlimited-credit charges. Fargate counts are bounded 0..3 per service in this profile; Terraform is not a billing safety system.

Official sources: [EC2 pricing](https://aws.amazon.com/ec2/pricing/on-demand/), [AWS's US East T3 example](https://docs.aws.amazon.com/prescriptive-guidance/latest/optimize-costs-microsoft-workloads/right-size-selection.html), [Fargate pricing](https://aws.amazon.com/fargate/pricing/), [AWS Fargate unit-rate example](https://aws.amazon.com/blogs/aws-cloud-financial-management/a-finops-guide-to-comparing-containers-and-serverless-functions-for-compute/), [PrivateLink](https://aws.amazon.com/privatelink/pricing/), [EBS](https://aws.amazon.com/ebs/pricing/). The r6i/gp3/endpoint/secret/key planning inputs require regional confirmation before a spend request; they are not live quotes.
