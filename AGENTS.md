# terraform-provider-celerdatabyoc

Terraform Provider for CelerData Cloud BYOC (Bring Your Own Cloud). Manages CelerData cloud clusters and related infrastructure resources via Terraform.

## Project Overview

- **Language**: Go 1.21
- **Framework**: HashiCorp Terraform Plugin SDK v2 (v2.26.1)
- **Authentication**: OAuth 2.0 Client Credentials (Application Key and Service Account)
- **Supported Clouds**: AWS, Azure, GCP
- **Current Version**: 0.8.4

## Directory Structure

```
terraform-provider-celerdatabyoc/
├── main.go                    # Entry point, starts Terraform Plugin gRPC server
├── celerdatabyoc/             # Provider definition, all Resources and Data Sources
│   ├── provider.go            # Provider schema definition, auth configuration
│   ├── resource_*.go          # Resource implementations (~26)
│   └── data_*.go              # Data Source implementations (3)
├── celerdata-sdk/             # Internal SDK / HTTP client library
│   ├── client/                # HTTP client wrapper (rate limiting, retry, polling)
│   ├── config/                # Configuration and OAuth authentication
│   ├── version/               # API version constants
│   ├── retries/               # Retry and polling logic (exponential backoff)
│   ├── useragent/             # User-Agent header construction
│   └── service/               # Domain-specific API services
│       ├── cluster/           # Cluster management API (lifecycle, scaling, users, config)
│       ├── credential/        # Credential management
│       ├── csp/               # Cloud service provider operations
│       ├── network/           # Network configuration
│       └── ranger-config/     # Ranger security configuration
├── common/                    # Shared utilities (constants, validators, version info)
├── internal/acctest/          # Acceptance test helpers
├── docs/                      # Terraform Registry documentation
│   ├── index.md               # Provider documentation index
│   ├── guides/                # Deployment guides (AWS / Azure / GCP)
│   ├── resources/             # Resource documentation
│   └── data-sources/          # Data Source documentation
├── Makefile                   # Build / install / test
├── .goreleaser.yml            # GoReleaser release configuration
└── .github/workflows/         # CI/CD (release.yml triggered by git tags)
```

## Core Resources

### Cluster Management
- `celerdatabyoc_classic_cluster` — Classic cluster
- `celerdatabyoc_elastic_cluster_v2` — Elastic cluster (current)
- `celerdatabyoc_elastic_cluster` — Elastic cluster (deprecated, use v2)

### Cluster Configuration
- `celerdatabyoc_cluster_user` — Database user management
- `celerdatabyoc_cluster_endpoints` — Cluster endpoint allocation
- `celerdatabyoc_cluster_domain_ssl_cert` — SSL certificate configuration
- `celerdatabyoc_cluster_modify_volume_detail` — Storage volume modification
- `celerdatabyoc_auto_scaling_policy` — Warehouse auto-scaling policy
- `celerdatabyoc_ranger_config` — Ranger security configuration

### AWS Resources
- `celerdatabyoc_aws_data_credential` / `_policy` — Data credentials and policies
- `celerdatabyoc_aws_deployment_credential_assume_policy` / `_policy` — Deployment credential policies
- `celerdatabyoc_aws_deployment_role_credential` — Deployment role credential
- `celerdatabyoc_aws_network` — Network configuration

### Azure Resources
- `celerdatabyoc_azure_data_credential` / `_deployment_credential` — Credentials
- `celerdatabyoc_azure_network` — Network configuration

### GCP Resources
- `celerdatabyoc_gcp_data_credential` / `_deployment_credential` — Credentials
- `celerdatabyoc_gcp_network` — Network configuration

### Data Sources
- `celerdatabyoc_aws_data_credential_assume_policy` — AWS policy template
- `celerdatabyoc_data_cluster_volume_detail` — Cluster volume information
- `celerdatabyoc_clusters` — List all clusters

## Development Guide

### Build & Install

```bash
# Build
make build

# Install to local Terraform plugin directory (~/.terraform.d/plugins/)
make install

# Create a snapshot release
make release
```

### Testing

```bash
# Unit tests
make test

# Acceptance tests (requires environment variables)
export CELERDATA_HOST="https://cloud-api.celerdata.com"
export CELERDATA_CLIENT_ID="your-client-id"
export CELERDATA_CLIENT_SECRET="your-client-secret"
make testacc
```

### Release

Releases are triggered by pushing a git tag via GitHub Actions:
```bash
git tag v0.x.x
git push origin v0.x.x
```

## Code Conventions & Patterns

### Resource Implementation Pattern

Each resource file (`resource_*.go`) follows a standard pattern:
1. Define a `resourceXxx()` function returning `*schema.Resource`
2. Implement CRUD functions: `CreateContext`, `ReadContext`, `UpdateContext`, `DeleteContext`
3. Use `ForceNew` in `Schema` for fields that trigger resource recreation
4. Mark sensitive fields with `Sensitive: true`
5. Mark computed fields with `Computed: true`
6. Implement `Importer` to support `terraform import`

### SDK Service Pattern

Each service directory under `celerdata-sdk/service/` contains:
- `api.go` — Interface definition + implementation
- `entity.go` — Request/response data structures (`*Req` / `*Resp` naming convention)

### Async Operation Handling

Long-running operations (e.g., cluster deployment, scaling) use `retries.Poll[T]()` with exponential backoff (500ms ~ 10s). The timeout for cluster deploy/scale operations is 6 hours.

### Error Handling

- SDK layer uses gRPC-style status codes (e.g., `codes.NotFound`)
- Resource layer uses Terraform Diagnostics for user-friendly error messages
- `hashicorp/go-multierror` is used for error aggregation

### Provider Authentication

Provider configuration parameters:
- `host` — API endpoint (default: `https://cloud-api.celerdata.com`, env: `CELERDATA_HOST`)
- `client_id` — OAuth Client ID (env: `CELERDATA_CLIENT_ID`)
- `client_secret` — OAuth Client Secret (env: `CELERDATA_CLIENT_SECRET`)

### Versioning

- Provider version is managed via the `VERSION` variable in `Makefile`
- SDK version is defined in `celerdata-sdk/version/version.go`
- API version constant in `celerdata-sdk/version/version.go` (currently `API_1_0 = "1.0"`)

## Documentation

- Provider documentation index: `docs/index.md`
- Cloud-specific deployment guides: `docs/guides/`
- Resource reference docs: `docs/resources/`
- Data Source reference docs: `docs/data-sources/`

Documentation follows the Terraform Registry standard format with argument descriptions, attribute descriptions, and usage examples.
