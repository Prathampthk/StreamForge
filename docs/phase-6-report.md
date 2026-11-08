# Phase 6 offline preparation report

**Status: offline preparation completed; AWS deployment acceptance is not complete.** The user explicitly chose “Keep offline; finish preparation first.” No AWS credentials/profile existed, and no authenticated plan, provisioning, image publication, paid environment, live cloud test or resource deletion was performed. Local validation must not be presented as a deployed portfolio service.

## Implemented

- Fail-closed verified TLS/custom trust roots for ClickHouse/PostgreSQL and shared TLS/SCRAM-SHA-512 Kafka options across ingest, consumer, quarantine and replay. Credentials/custom CA without TLS and malformed CA are rejected; no insecure-verification switch. Local Compose remains loopback/plaintext.
- Go 1.26.10 and compatible patched PostgreSQL, telemetry, gRPC and x/* dependencies. Initial reachable scan found 14 vulnerabilities; fixes and the image scan's additional x/net/gRPC findings were addressed rather than suppressed. Go/Docker/scanner/action/provider versions are pinned; runtime Go base/scanner are digest-pinned and GitHub actions use immutable commits.
- Native Terraform modules: private network/endpoints, explicit security groups, per-service secret containers and IAM, immutable ECR, two stateful EC2 hosts/retained encrypted EBS, private Fargate services/operator replay task, private discovery, scoped logs/flow logs, optional ACM/HTTPS ALB and a separate protected remote-state bootstrap. Public access, services/data hosts and publisher roles start disabled. No secret values enter Terraform variables/state/user-data. No NAT, RDS or HA claim.
- Bootstrap/secrets/image tooling: private offline CA/credential generation, verified leaf names, first-boot ownership/filesystem/schema guards, runtime secret retrieval, retained-disk startup, explicit image scan-before-push, account-checked uploads and a plan-only helper. Changed schema/unsafe existing filesystems fail visibly. These scripts are prepared/render-checked, not proven on a real AMI or AWS disk.
- PR/main CI for contracts, race/static/security checks, fresh isolated real-service integration, database privilege checks, infrastructure plans, images and reproducibility. Optional main-only manual ECR publisher uses a branch-restricted environment/OIDC role, explicit confirmation and no deployment/IAM/state authority. Additional reviewers are optional; no second-person approval is imposed on a sole-owner workflow. No apply/deploy/destroy workflow exists.
- Deployment, migration/rotation, costs, teardown/retention, final diagrams, README and evidence-based resume/interview material. Phase 5's actual degraded-baseline throughput and failed analytics/latency/scaling gates remain visible; these numbers were not re-benchmarked with the patched toolchain.

## Commands and actual results

| Check | Actual result and boundary |
|---|---|
| Go unit/race tests and go vet after dependency fixes | PASS; TLS trust/rejection and Kafka credential-without-TLS cases included |
| `govulncheck@v1.1.4 ./...` | PASS, zero reachable vulnerabilities; report also notes uncalled/imported-module findings, so not “no vulnerabilities anywhere” |
| Isolated full integration suite | PASS after the fresh-topic leader wait fix; final patched-dependency rerun: **85.368s**. This is suite duration, not a performance measurement |
| Terraform init/validate | PASS with provider 6.72.0, both dev and remote-state bootstrap; backend/credentials absent for offline checks |
| Terraform fmt and `terraform test` | PASS, five mocked plan cases: private bootstrap, missing-image rejection, unrestricted-ingress rejection, private enabled services and opt-in HTTPS |
| `make cloud-check` | PASS for offline schemas/plans, leaf validation/rendering and actionlint; no AWS API calls |
| Certificate/render check | PASS for broker/metadata/ClickHouse leaf names, wrong-host rejection, private file modes and secure listeners; temp keys removed |
| Workflow actionlint 1.7.12 / Python compile / shell syntax | PASS; this does not prove GitHub/AWS environment settings exist |
| Independent binaries | PASS, all five binaries identical across paired builds with fixed flags |
| Independent ingest runtime image | PASS on GitHub with pinned BuildKit, normalized root filesystem metadata and provenance disabled; paired image ID `sha256:1f62a12ba2a9004aa8c9bd3c0505b1dfd21b751bc666cbb4f3b4fb9309ccaf93` at source commit `9b3a6c9`; other images are built/scanned by CI, not claimed byte-compared |
| Trivy ingest image | PASS, zero HIGH/CRITICAL findings detected after additional dependency patches; vendor-image scan success is not claimed |
| Trivy default private IaC profile | PASS, zero HIGH/CRITICAL findings detected; no blanket suppressions |
| Disposable ClickHouse privilege check | PASS: aggregate/refresh reads allowed; query raw read/insert denied; consumer insert/MV/readiness allowed; consumer raw read and DDL denied. Uses only its own tmpfs instance and leaves benchmark volumes untouched |
| Authenticated AWS plan / live IAM-SG negative checks / smoke / restart / load / cleanup | NOT RUN, intentionally offline; mocks are not substitutes |

Local raw command logs are retained privately under ignored `.aws-artifacts/`. Small machine-readable validation evidence is committed under `docs/evidence/`. [GitHub-hosted CI](https://github.com/Prathampthk/StreamForge/actions/runs/42434743569) passed all six jobs on source commit `9b3a6c97ec5721c2fe98f177dda86920a763ee13`: contracts/race/static/vulnerability checks, binary reproducibility, fresh real-service integration and database privileges, infrastructure, and all four application image security gates. The ingest job also passed independent runtime image reproducibility. The hosted integration suite completed in 78.116s; its disposable privilege check also passed. This report-only follow-up does not change that tested source.

## Failures found and corrected

- A fresh isolated suite exposed asynchronous Kafka leader availability immediately after CreateTopics. The test now waits for actual readiness rather than expecting immediate metadata propagation. The production readiness contract was not weakened.
- Initial IaC validation caught resource-shape/type errors; native HCL plus the pinned provider and safety tests now validate. A scanner representation issue on equivalent Terraform JSON nested blocks was resolved by native HCL, not by ignoring ECR scan-on-push findings. Customer-KMS artifact encryption addressed the substantive bucket finding.
- Independent image builds first differed in the CA-layer filesystem timestamps although their binary layers matched. Local timestamp rewriting initially passed, but the hosted Docker exporter still differed. Normalizing every runtime file/directory and selecting a digest-pinned BuildKit container exporter fixed the hosted paired build too; reproducibility is scoped to the documented compiler/platform/epoch/provenance settings.
- Isolated database setup first hit temporary-init-server/default-database assumptions; readiness now waits for the final daemon and applies migrations to the intended database. The check then exposed invalid multi-table GRANT syntax and legacy MV invoker source-read requirements. Cloud bootstrap now uses separate grants and explicit trusted DEFINER execution, keeping consumer raw reads denied. Existing benchmark schemas were not modified.
- An attempt to target existing benchmark data for privilege testing was rejected by automatic approval review. It was replaced by a disposable tmpfs server; no benchmark-volume write was executed and no action remains blocked by that rejection.

## Acceptance and limitations

Offline source/configuration/cryptographic/real-local-service checks pass within the recorded scope. The requested live AWS acceptance criteria are **unverified**, not passed: account-specific plan, AMI/prerequisites, device mount/retained-volume replacement, actual service bootstrap, IAM/SG/ACL enforcement, restart/redeploy, specified-instance cloud load and cleanup inventory. Vendor publication must pass all seven image scans at execution time; a scan failure blocks before first push.

This is a single-AZ dev profile. Kafka+PostgreSQL share a node and administrative boundary; Kafka RF=1 and one ClickHouse instance are not HA. Roles use service-specific secret/image/data scopes; required token/channel actions retain documented `Resource=*` API scope. Public HTTPS terminates at ALB with a private HTTP target hop. Leaf certificate rotation is manual (30-day leaves), as are backups, schema upgrades and server/client credential coordination. Stateful app-only releases do not change node image inputs; keep the reviewed bootstrap bundle revision fixed unless changing it deliberately.

Retained EBS/secrets/buckets/KMS/OIDC guards and ALB deletion protection require reviewed teardown changes. Stopping compute alone leaves bills. Cost assumptions are illustrative, approximately $230–$300/month for a low-volume private demo left running; no regional quote, budget cap, credits or spend approval is claimed. See [cost review](aws/costs.md).

The growing canonical-refresh bottleneck remains unresolved: the historical local run verified canonical facts, while final revenue/traffic snapshots, live query/freshness, ingest p95 and 2x scaling targets failed. Preparing more infrastructure does not change those findings.

## Handoff

The offline package is ready for review. No later AWS action is implied by this report. When the user chooses to leave offline mode, first select/connect an authenticated account/profile/region, prepare the real read-only plan and current cost quote, then request separate approval for its exact external actions. See [deployment](aws/deployment.md), [teardown](aws/teardown.md), [architecture](final-architecture.md) and [resume evidence](resume-evidence.md).

## Files changed in the offline Phase 6 preparation

- `.dockerignore`
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.gitignore`
- `.go-version`
- `Makefile`
- `README.md`
- `deploy/compose.ci.yaml`
- `deploy/compose.consumer.yaml`
- `deploy/docker/consumer.Dockerfile`
- `deploy/docker/ingest.Dockerfile`
- `deploy/docker/query.Dockerfile`
- `deploy/docker/tools.Dockerfile`
- `deploy/vendor-images.json`
- `docs/adr/008-aws-deployment.md`
- `docs/aws/costs.md`
- `docs/aws/deployment.md`
- `docs/aws/teardown.md`
- `docs/evidence/phase-6-offline.json`
- `docs/final-architecture.md`
- `docs/phase-6-report.md`
- `docs/resume-evidence.md`
- `go.mod`
- `go.sum`
- `infra/terraform/bootstrap/.terraform.lock.hcl`
- `infra/terraform/bootstrap/main.tf`
- `infra/terraform/envs/dev/.terraform.lock.hcl`
- `infra/terraform/envs/dev/backend.tf.example`
- `infra/terraform/envs/dev/deployment.tfvars.json.example`
- `infra/terraform/envs/dev/main.tf`
- `infra/terraform/envs/dev/tests/safety.tftest.hcl`
- `infra/terraform/modules/ecs-services/main.tf`
- `infra/terraform/modules/network/main.tf`
- `infra/terraform/modules/registry/main.tf`
- `infra/terraform/modules/security/main.tf`
- `infra/terraform/modules/stateful/main.tf`
- `infra/terraform/modules/stateful/user-data.sh.tftpl`
- `internal/analytics/read.go`
- `internal/analytics/sink.go`
- `internal/broker/consumer.go`
- `internal/broker/producer.go`
- `internal/broker/security.go`
- `internal/broker/security_test.go`
- `internal/config/consumer.go`
- `internal/config/query.go`
- `internal/metadata/store.go`
- `internal/platform/tls.go`
- `internal/platform/tls_test.go`
- `internal/replay/quarantine.go`
- `internal/replay/runner.go`
- `scripts/aws/check-db-grants.py`
- `scripts/aws/check-publish-environment.py`
- `scripts/aws/check-render.py`
- `scripts/aws/plan.sh`
- `scripts/aws/prepare-secrets.py`
- `scripts/aws/publish-images.py`
- `scripts/aws/stateful.py`
- `scripts/aws/upload-secrets.py`
- `scripts/go.sh`
- `scripts/integration-isolated.sh`
- `scripts/reproducible-image.sh`
- `scripts/reproducible.sh`
- `scripts/terraform.sh`
- `tests/integration/ingest_test.go`
