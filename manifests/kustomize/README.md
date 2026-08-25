# Kubeflow Pipelines Kustomize Manifests

Kubeflow Pipelines can be installed standalone and as part of the [community distribution](https://github.com/kubeflow/community-distribution).
[Installation Options for Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/operator-guides/installation/).

## Custom artifact-store endpoints

The UI server only accepts secret-backed S3-compatible `bucketProviders` whose
HTTP(S) origin matches the operator-configured MinIO or AWS endpoint. Add any
additional origins to `ALLOWED_ARTIFACT_ENDPOINTS` in `pipeline-install-config`.
Entries are comma-separated absolute origins, including the scheme and optional
port, for example `https://objects.example.com:9443`; paths and credentials are
not accepted. HTTP origins must be listed as HTTP and should only be used for
trusted in-cluster stores.

Upgrades from releases that allowed arbitrary provider endpoints must configure
this allowlist before users can read artifacts from a custom store. Rejected
requests return HTTP 400 and identify `ALLOWED_ARTIFACT_ENDPOINTS` as the
required operator setting. Official regional AWS S3 service endpoints are
trusted as a group only when `AWS_S3_ENDPOINT` is explicitly configured to an
official AWS S3 service endpoint; otherwise list each required origin.
Profile-created artifact proxies intentionally do not inherit another UI
server's object-store environment, so custom stores must also be listed in
`ALLOWED_ARTIFACT_ENDPOINTS` for those proxies. UI deployments that configure
`AWS_S3_ENDPOINT` directly may put the port in that value or set
`AWS_S3_PORT`; if both specify a port, they must agree.
