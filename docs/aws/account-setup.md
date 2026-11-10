# AWS account setup — Free plan preserved

## Current status

The following is **user-reported setup**, not an authenticated CLI/account audit. Codex has not inspected credentials or verified the live account. Console sign-in and CLI authentication are distinct.

| Field | Status |
|---|---|
| AWS account ID | `788080238947` |
| Workload region | `us-east-2` (Ohio) |
| Account plan | AWS Free plan with promotional credits; preserve this plan |
| AWS CLI | v2.39.7 installed locally, as reported |
| Development identity | IAM user `streamforge-dev`; console login verified by the user |
| Development permissions | AWS-managed `ReadOnlyAccess`, as reported |
| Root MFA | Enabled using a passkey, as reported |
| Development-user MFA | Enabled, as reported |
| Billing budget | $5 USD monthly; budget email verified by the user |
| Organizations / IAM Identity Center | Not configured; explicitly deferred |
| SSO portal / SSO region / CLI SSO profile | Not available / not configured |
| Authenticated CLI profile / caller identity | Not configured / not verified |
| StreamForge cloud deployment | None; user reports no intentionally provisioned AWS resources |

The IAM console user is a temporary alternative, not an IAM Identity Center SSO identity. The name `streamforge-dev` identifies the IAM user and a proposed future CLI profile; it does not establish that a profile exists. The following commands have **not** completed successfully and are not prerequisites for local development:

```sh
aws configure sso --profile streamforge-dev
aws sso login --profile streamforge-dev
aws sts get-caller-identity --profile streamforge-dev --query Account --output text
```

## Preserve the Free plan and approval boundaries

The user encountered an AWS warning that creating an organization would upgrade the account and expire its Free Tier credits. AWS documents automatic plan upgrades associated with Organizations and the credit-expiry exception; see [plan rules](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html) and [Free Tier FAQ](https://aws.amazon.com/free/free-tier-faqs/). Do not create or join an organization, enable IAM Identity Center, or upgrade the plan without explicit approval. Do not treat Organizations as a routine account-setup prerequisite.

Do not create IAM access keys, request passwords, or collect/store credentials, tokens, authorization links or MFA codes. Do not inspect the user's credential files or caches. No authenticated AWS actions, permission changes, infrastructure provisioning, image/secret publication or billable-resource creation are authorized by this status update. A reviewed concrete plan and explicit approval must precede cloud changes.

The $5 budget is an alert threshold, **not** a deployment-spend authorization or universal hard cap. Credit balance, expiry and Free-plan service eligibility have not been independently checked. The prepared topology has substantial ongoing costs; see [cost assumptions](costs.md). No assumption of free deployment or credit coverage is justified.

## Proposed CLI alternative — not configured or executed

AWS documents browser-based `aws login` for IAM users on CLI v2.32+. The reported v2.39.7 meets that minimum. This route needs no Organizations or Identity Center instance and no long-lived access key. It requires `SignInLocalDevelopmentAccess`, which is not reported attached; `ReadOnlyAccess` alone is not the documented sign-in prerequisite. Any policy addition needs a separately reviewed owner action.

If later approved, the user would authenticate directly in their browser as the MFA-protected IAM user, select Ohio, then run:

```sh
# Proposal only; do not execute as part of this documentation update.
aws login --profile streamforge-dev --region us-east-2
aws sts get-caller-identity --profile streamforge-dev --region us-east-2
aws logout --profile streamforge-dev
```

Verify both account and IAM-user ARN before use. The CLI caches temporary login credentials locally; this requires explicit agreement under the current no-credential-storage instruction. Codex must not read, copy or commit that cache. SDK compatibility can require a separate `credential_process` profile; never print credential-export output. See [AWS console-based local login](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html).

The current `scripts/aws/plan.sh` mounts the host AWS directory read-only into a Terraform container without AWS CLI. Its compatibility with browser-login credentials is **unverified**; a host credential-process command cannot simply be assumed to work inside that container. Review and test a secure credential-provider arrangement separately before any authenticated Terraform plan. No wrapper or authentication configuration has been changed here.

## Incomplete and blocked work

| Work | Blocker / permitted next action |
|---|---|
| Identity Center portal, permission sets, account assignment and `aws configure sso` / `aws sso login` | Specifically blocked by deferred Identity Center. Remain deferred unless explicitly approved. |
| CLI caller-account/ARN verification and read-only resource/permission inspection | Blocked by missing approved CLI authentication, not inherently by missing SSO. The proposed browser-login route could address this after review. |
| Live Terraform plan and remote-state access | Need approved authentication, credential-provider compatibility, backend prerequisites and explicit plan authorization. Read-only permissions do not permit bootstrap creation or imply all required reads will succeed. |
| AWS service eligibility and current regional cost review | Public/offline research can proceed; account-specific checks need approved access or user-supplied console information. |
| State bootstrap, deployment, OIDC publisher role, image/secret publication and cloud recovery tests | Not authorized. Need reviewed resources, permissions, costs, duration, retention/cleanup choices and explicit approval. SSO alone would not authorize these. |
| Local analytics development, fixtures, benchmarks, recovery rehearsals and ordinary CI | No AWS authentication or Identity Center dependency. Continue locally. |

The next recommended development step is an isolated incremental-analytics prototype that bounds work by new/changed identities while preserving exact global-ID conflicts and withdrawal. Keep retained data unchanged until correctness and scaling evidence justify a separately reviewed migration. See [local completion order](../offline-readiness.md#remaining-local-completion-order).
