# LOCAL ADAPTIX REPO GOVERNANCE

This repository inherits global governance from:

Adaptix-Governance/AGENTS.md

## Local repo scope

Repo name: Adaptix-Temporal-Service
Domain: Temporal
Owner service: adaptix-temporal-service

## Local execution rule

This repo must comply with:

- GLOBAL_EXECUTION_POLICY.md
- GLOBAL_CHANGE_CLASSIFICATION.md
- GLOBAL_CHANGE_LIFECYCLE_POLICY.md
- GLOBAL_NO_FAKE_SUCCESS_POLICY.md
- GLOBAL_PRODUCTION_READINESS.md
- GLOBAL_ROUTE_POLICY.md
- GLOBAL_SERVICE_POLICY.md
- GLOBAL_AUTH_POLICY.md
- GLOBAL_TENANT_POLICY.md
- GLOBAL_SCHEMA_POLICY.md
- GLOBAL_OBSERVABILITY_POLICY.md
- GLOBAL_SECURITY_POLICY.md
- GLOBAL_EVIDENCE_POLICY.md
- GLOBAL_CLEARINGHOUSE_BOUNDARY.md

## Change lifecycle rule

Any change made in this repository must complete the full Adaptix change lifecycle:

1. Commit the change.
2. Push the branch to GitHub.
3. Monitor for merge conflicts.
4. Resolve merge conflicts.
5. Monitor GitHub Actions / CI.
6. Correct validation or CI failures.
7. Merge only after required checks pass.
8. Deploy to AWS where applicable.
9. Monitor AWS deployment health.
10. Monitor ECS tasks, services, logs, target groups, gateway/API behavior, and service health.
11. Correct runtime failures.
12. Redeploy if corrections are required.
13. Repeat monitoring until stable.
14. Record completion evidence.

A change is not complete because it was edited locally.

A change is complete only when it is committed, pushed, conflict-checked, merged, deployed where applicable, monitored, corrected for failures, and verified stable with evidence.

## First rollout restriction

This local AGENTS.md file is governance inheritance only.

Do not modify production logic as part of this first rollout.

## Local override rule

Local rules may be stricter than global rules.

Local rules may never weaken global rules.

## Provider boundary

This repository inherits Adaptix-Governance/GLOBAL_CLEARINGHOUSE_BOUNDARY.md.

- Stedi is the live healthcare clearinghouse / EDI transport.
- Office Ally remains onboarding-only unless the founder explicitly changes this boundary.
- TrustSign is the only AdaptixCore e-signature system. Do not introduce Dropbox Sign, HelloSign, or DocuSign.
- PostGrid owns printed statements / physical mail.
- Telnyx is limited to billing communications unless the founder expands scope.
- Do not claim Tier 5, production-ready, live, or healthy without runtime proof.
