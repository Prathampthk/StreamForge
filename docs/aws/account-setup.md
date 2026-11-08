# AWS account information for the deployment plan

No StreamForge cloud resources have been provisioned. You can prepare authentication while local analytics validation continues. The first cloud step will inspect/plan; publication and provisioning require separate approval of actual resources and spend.

## Prepare a development login

For your personal account, finish account registration and enable [root-user MFA](https://docs.aws.amazon.com/IAM/latest/UserGuide/enable-mfa-for-root.html). Use a separate development identity for project work. Install current [AWS CLI v2 for macOS](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

Preferred path: follow [IAM Identity Center setup](https://docs.aws.amazon.com/singlesignon/latest/userguide/getting-started.html). For AWS-account/CLI access, use an **organization instance**; an account-only instance does not provide the same AWS-account access portal. This may require AWS Organizations setup in your own management account. Create your development user, a permission set using the AWS-managed `ReadOnlyAccess` policy for the initial inspection/plan, and assign that user and permission set to your account. If this is a school/company account, use its provided access instead of changing its organization.

The Identity Center dashboard provides the AWS access portal URL. Note its **SSO region**, which can differ from the workload region. Configure locally:

```sh
aws configure sso --profile streamforge-dev
aws sso login --profile streamforge-dev
aws sts get-caller-identity --profile streamforge-dev --query Account --output text
```

The wizard asks for the portal URL, SSO region, account, permission set/role, default workload region and profile name. Use `us-east-2` (Ohio) as the workload region to match the prepared configuration, unless you prefer another region. See the [AWS SSO CLI guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html).

If you already have a supported non-root console identity, AWS CLI v2.32+ also supports `aws login --profile streamforge-dev`. That identity needs `SignInLocalDevelopmentAccess`; see [console-based local login](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html). We will verify Terraform credential compatibility before using this route. No long-lived access key is needed for either route.

## Information to send

```text
Account ID: <12 digits from the command above or console account menu>
Profile: streamforge-dev
Workload region: us-east-2
Maximum total demo spend: <your amount>
Demo duration: <hours/days>
Access: private initially, or public HTTPS later
```

Do not send passwords, access keys, session tokens, browser authorization links or MFA codes. Authentication stays on your computer.

Create a cost alert under **Billing and Cost Management → Budgets** using your chosen amount; see [AWS Budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-create.html). Alerts are not a universal hard spending cap. This topology includes billable services; no credits or free-tier coverage is assumed. We will prepare a region/account-specific cost review before provisioning.

There is no need to manually create EC2, ECS, databases, VPCs, ECR repositories, SSH keys or certificates now. The plan selects the infrastructure inputs. A private demo needs no domain; public HTTPS later needs reviewed DNS/certificate/client-CIDR inputs. GitHub publication can later use an OIDC role rather than stored AWS keys.
