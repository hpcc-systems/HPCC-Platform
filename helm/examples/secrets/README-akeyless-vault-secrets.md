# Containerized HPCC Systems Vault Secrets using Akeyless Vault

This example demonstrates HPCC use of Akeyless Vault for managing secrets using access key authentication.

This example assumes you are starting from a Linux command shell in the HPCC-Platform/helm directory.  From there you will find the example files and this README file in the examples/secrets directory.

## Akeyless Vault support

HPCC Systems supports Akeyless as an alternative to Hashicorp Vault for secret storage and retrieval.  Akeyless uses access key authentication (access ID + access key) rather than the appRole, Kubernetes, or client certificate methods used by Hashicorp Vault.

## Prerequisites

- An Akeyless account (SaaS at https://www.akeyless.io or a self-hosted Akeyless Gateway)
- An Akeyless access key pair (access ID and access key)
- The Akeyless CLI (optional, for creating and managing secrets)

### Install the Akeyless CLI

https://docs.akeyless.io/docs/cli

--------------------------------------------------------------------------------------------------------

## Setting up Akeyless

### Authenticate with the Akeyless CLI

```bash
akeyless auth --access-id p-xxxxxx --access-key <your-access-key>
```

### Create secrets

Create example secrets for use with HPCC.  Akeyless secrets are stored as key-value JSON objects.

Create an 'eclUser' secret (accessible directly from ECL code):

```bash
akeyless create-secret --name /hpcc/eclUser/vault-example --value '{"crypt.key":"<base64-encoded-key-value>"}'
```

Create an 'ecl' secret for HTTP-CONNECT (used internally by HTTPCALL/SOAPCALL):

```bash
akeyless create-secret --name /hpcc/ecl/http-connect-vaultsecret --value '{"url":"http://example.com/api","username":"myuser","password":"mypass"}'
```

Create a 'git' secret (for Git repository access tokens):

```bash
akeyless create-secret --name /hpcc/github_access/my-repo-token --value '{"token":"ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}'
```

--------------------------------------------------------------------------------------------------------

## Configuring HPCC to use Akeyless

### Vault configuration properties

The following properties are used to configure an Akeyless vault entry:

| Property | Required | Description |
|---|---|---|
| `name` | Yes | Name used to identify this vault within the platform |
| `url` | Yes | Base URL of the Akeyless API (e.g. `https://api.akeyless.io` for SaaS) |
| `type` | Yes | Must be set to `akeyless` |
| `accessId` | Yes | Akeyless access ID (e.g. `p-xxxxxx`).  Supports `${env.VAR}` substitution. |
| `accessKey` | Conditional | Akeyless access key.  Required unless `client-secret` is provided.  Supports `${env.VAR}` substitution. |
| `client-secret` | Conditional | Name of a Kubernetes secret containing an `access-key` key.  Required unless `accessKey` is provided.  Supports credential rotation. |
| `accessType` | No | Authentication type (defaults to `access_key`, which is the only supported type) |
| `namespace` | No | Path prefix prepended to all secret names (e.g. `hpcc/eclUser/`) |
| `verify_server` | No | Enable TLS server certificate verification (default: `true`, set to `false` for testing) |

**Note:** It is not necessary to set `kind` for Akeyless vaults.  When `type` is set to `akeyless`, the `kind` is automatically derived as `akeyless_v2`.

### Authentication methods

Akeyless credentials can be provided in two ways:

**1. Direct access key** — The access key is specified directly in the values file:

```yaml
vaults:
  ecl:
    - name: my-akeyless-vault
      url: https://api.akeyless.io
      type: akeyless
      accessId: p-xxxxxx
      accessKey: <your-access-key>
      namespace: hpcc/ecl/
```

**2. Kubernetes secret (recommended for production)** — The access key is stored in a Kubernetes secret, allowing credential rotation without restarting HPCC components:

```bash
kubectl create secret generic akeyless-access-key --from-literal=access-key=<your-access-key>
```

```yaml
secrets:
  system:
    akeyless-access-key: akeyless-access-key

vaults:
  ecl:
    - name: my-akeyless-vault
      url: https://api.akeyless.io
      type: akeyless
      accessId: p-xxxxxx
      client-secret: akeyless-access-key
      namespace: hpcc/ecl/
```

When `client-secret` is used, the access key is re-read from the Kubernetes secret each time authentication is needed, picking up rotated credentials automatically.

### Environment variable substitution

The `accessId`, `accessKey`, and `url` properties support environment variable substitution using the `${env.VAR}` syntax:

```yaml
vaults:
  ecl:
    - name: my-akeyless-vault
      url: https://${env.AKEYLESS_GW_HOST}
      type: akeyless
      accessId: ${env.AKEYLESS_ACCESS_ID}
      accessKey: ${env.AKEYLESS_ACCESS_KEY}
      namespace: hpcc/ecl/
```

### Namespace configuration

The `namespace` property is prepended to all secret names when making requests to Akeyless.  Leading slashes are automatically stripped from the namespace.

For example, with `namespace: hpcc/ecl/` and a secret request for `my-secret`, the full secret path sent to Akeyless would be `hpcc/ecl/my-secret`.

If your secrets are stored at the root of your Akeyless account, omit the `namespace` property.

--------------------------------------------------------------------------------------------------------

## Example: Full values file with multiple vault categories

```yaml
secrets:
  system:
    akeyless-access-key: akeyless-access-key

vaults:
  ecl:
    - name: my-ecl-vault
      url: https://api.akeyless.io
      type: akeyless
      accessId: p-xxxxxx
      client-secret: akeyless-access-key
      namespace: hpcc/ecl/
  eclUser:
    - name: my-eclUser-vault
      url: https://api.akeyless.io
      type: akeyless
      accessId: p-xxxxxx
      client-secret: akeyless-access-key
      namespace: hpcc/eclUser/
  git:
    - name: my-git-vault
      url: https://api.akeyless.io
      type: akeyless
      accessId: p-xxxxxx
      client-secret: akeyless-access-key
      namespace: hpcc/github_access/
```

## Configuring Akeyless in environment.xml (bare-metal / VM deployments)

For non-containerized deployments, Akeyless vaults are configured in the `<vaults>` section under `<Environment><Software>` in the environment.xml file. The XML attributes correspond directly to the YAML properties described above.

### Single vault category

```xml
<Environment>
  <Software>
    <vaults>
      <git name="my-git-vault"
           type="akeyless"
           url="https://api.akeyless.io"
           accessId="p-xxxxxx"
           accessKey="your-access-key"
           namespace="hpcc/github_access/"
           verify_server="0"/>
    </vaults>
  </Software>
</Environment>
```

### Multiple vault categories

```xml
<Environment>
  <Software>
    <vaults>
      <ecl name="my-ecl-vault"
           type="akeyless"
           url="https://api.akeyless.io"
           accessId="p-xxxxxx"
           accessKey="your-access-key"
           namespace="hpcc/ecl/"/>
      <eclUser name="my-eclUser-vault"
               type="akeyless"
               url="https://api.akeyless.io"
               accessId="p-xxxxxx"
               accessKey="your-access-key"
               namespace="hpcc/eclUser/"/>
      <git name="my-git-vault"
           type="akeyless"
           url="https://api.akeyless.io"
           accessId="p-xxxxxx"
           accessKey="your-access-key"
           namespace="hpcc/github_access/"/>
    </vaults>
  </Software>
</Environment>
```

The element name (`ecl`, `eclUser`, `git`, etc.) determines the vault category, just as the YAML key does in the Helm chart.

### Using environment variables

The `url`, `accessId`, and `accessKey` attributes support environment variable substitution using the `${env.VAR}` syntax, where VAR is the name of the environment variable.  This avoids storing credentials directly in the configuration file. See example below:

```xml
<vaults>
  <git name="my-git-vault"
       type="akeyless"
       url="https://${env.AKEYLESS_GW_HOST}"
       accessId="${env.AKEYLESS_ACCESS_ID}"
       accessKey="${env.AKEYLESS_ACCESS_KEY}"
       namespace="hpcc/github_access/"/>
</vaults>
```

Set the environment variables before starting the HPCC components:

```bash
export AKEYLESS_GW_HOST=api.akeyless.io
export AKEYLESS_ACCESS_ID=p-xxxxxx
export AKEYLESS_ACCESS_KEY=your-access-key
```

**Note:** The `client-secret` option availabe in a containerized environments is not available in bare-metal deployments since it relies on Kubernetes-mounted secrets.

--------------------------------------------------------------------------------------------------------

## Installing HPCC with Akeyless vault secrets

Install the HPCC helm chart with the Akeyless vault configuration:

```bash
helm install myhpcc hpcc/ --set global.image.version=latest -f examples/secrets/values-secrets-akeyless.yaml
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods
```

--------------------------------------------------------------------------------------------------------

If you don't already have the HPCC client tools installed please install them now:

https://hpccsystems.com/download

## Using the created 'eclUser' category secrets directly in ECL code

The following ecl command will run the example ECL file that demonstrates accessing a vault secret directly from ECL code.

```bash
ecl run hthor examples/secrets/crypto_vault_secret.ecl
```

The expected result would be:

```xml
<Result>
<Dataset name='vault_message'>
 <Row><vault_message>For your eyes only</vault_message></Row>
</Dataset>
</Result>
```

## Using the created 'ecl' category secrets via HTTPCALL from within ECL code

The following ecl command will run the example ECL file that demonstrates an HTTPCALL that uses a vault secret for connection and authentication.

```bash
ecl run hthor examples/secrets/httpcall_vault.ecl
```

For each job the expected result would be:

```xml
<Result>
<Dataset name='Result 1'>
 <Row><authenticated>true</authenticated></Row>
</Dataset>
</Result>
```

--------------------------------------------------------------------------------------------------------

## Differences from Hashicorp Vault

| Aspect | Hashicorp Vault | Akeyless Vault |
|---|---|---|
| Configuration property | `kind: kv-v2` | `type: akeyless` |
| URL format | Includes full path with `${secret}` placeholder | Base API URL only (e.g., `https://api.akeyless.io`) |
| Authentication | appRole, Kubernetes, client cert, or token | Access key (access ID + access key) |
| Secret retrieval | HTTP GET to vault path | HTTP POST to `/get-secret-value` endpoint |
| Credential rotation | Depends on auth method | Via `client-secret` Kubernetes secret |
| Namespace handling | Prepended as-is | Leading slashes stripped, then prepended |

## Using Akeyless HVP Gateway (HashiCorp Vault Protocol Compatibility)

The Akeyless HVP (HashiCorp Vault Protocol) Gateway provides a compatibility layer that allows you to use Akeyless as a drop-in replacement for HashiCorp Vault while keeping your existing vault configuration structure. This approach is useful when you want to minimize configuration changes and leverage your existing Hashicorp Vault setup patterns.

### How the HVP Gateway works

The HVP Gateway translates HashiCorp Vault API requests to Akeyless API calls. Instead of rewriting your entire vault configuration, you update a few key attributes to point to the HVP Gateway endpoint and provide Akeyless credentials.

### Reconfiguring Hashicorp Vault to use Akeyless HVP Gateway

#### YAML configuration (Helm)

Update your Hashicorp Vault definitions to use the HVP Gateway:

**Before (Hashicorp Vault):**

```yaml
vaults:
  ecl:
    - name: my-vault
      kind: kv-v2
      url: https://vault.example.com/v1/<vault-engine>/data/${secret}
      appRoleId: 12345678-1234-1234-1234-123456789012
      appRoleSecret: vault-approle-secret
      namespace: original/team-a/prod/eclusers
```

**After (Akeyless HVP Gateway):**

```yaml
vaults:
  ecl:
    - name: my-vault
      kind: kv-v2
      url: https://hvp.akeyless.io/v1/secret/data/path/to/akeyless/key-storage/${secret}
      appRoleId: p-xxxxxx
      appRoleSecret: vault-approle-secret
```

#### XML configuration (bare-metal / VM)

Update your environment.xml to use the HVP Gateway:

**Before (Hashicorp Vault):**

```xml
<vaults>
  <ecl name="my-vault"
       kind="kv-v2"
    url="https://vault.example.com/v1/<vault-engine>/data/${secret}"
       appRoleId="12345678-1234-1234-1234-123456789012"
       appRoleSecret="vault-approle-secret"
    namespace="original/team-a/prod/eclusers"/>
</vaults>
```

**After (Akeyless HVP Gateway):**

```xml
<vaults>
  <ecl name="my-vault"
       kind="kv-v2"
  url="https://hvp.akeyless.io/v1/secret/data/path/to/akeyless/key-storage/${secret}"
       appRoleId="p-xxxxxx"
       appRoleSecret="vault-approle-secret"/>
</vaults>
```

### URL transformation rules

When migrating to HVP Gateway, transform your URL as follows:

1. **Replace the hostname and path** with the HVP Gateway base URL: `https://hvp.akeyless.io/v1/secret/data/`
2. **Append the Akeyless secret path** (path to the key in Akeyless)
3. **Append the `${secret}` placeholder**

The Akeyless secret path used in the HVP URL may not match the original Vault `namespace` value. Use the actual path where the key is stored in Akeyless.

**Example:**

- Original Hashicorp URL: `https://vault.example.com/v1/someengine/data/${secret}`
- Original namespace: `original/team-a/prod/eclusers`
- Akeyless key path: `path/to/akeyless/key-storage`
- HVP Gateway URL: `https://hvp.akeyless.io/v1/secret/data/path/to/akeyless/key-storage/${secret}`

### Configuration changes required

| Attribute | Change | Notes |
|---|---|---|
| `url` | Update to HVP Gateway URL | See URL transformation rules above |
| `appRoleId` | Replace Vault UUID with Akeyless Access ID | For example: `p-xxxxxx` instead of `12345678-1234-1234-1234-123456789012` |
| `kind` | Ensure it is `kv-v2` (with hyphen) | Change `kv_v2` (underscore) to `kv-v2` if needed |
| `namespace` | **Remove entirely** | Replace it with the Akeyless key path embedded in the URL; this path may differ from the original namespace value |
| `appRoleSecret` | Keep the same name | But update the secret's `secret-id` key to contain the Akeyless Access Key (see below) |

### Updating the Kubernetes Secret

If using `appRoleSecret` in a Kubernetes environment, update the secret to contain the Akeyless Access Key:

```bash
# Delete the old secret if it contains Vault credentials
kubectl delete secret vault-approle-secret

# Create a new secret with Akeyless Access Key
kubectl create secret generic vault-approle-secret --from-literal=secret-id=<your-akeyless-access-key>
```

For bare-metal/VM deployments, store the Akeyless Access Key in your secure credential store and ensure it's accessible to the HPCC components via the `appRoleSecret` reference.

### Advantages of using HVP Gateway

- **Minimal configuration changes** — Keep your existing vault structure and only update a few key fields
- **Credential migration** — Transition from Vault credentials to Akeyless credentials without restructuring
- **Compatibility** — Use Akeyless while maintaining Vault-like configuration patterns

### Testing the HVP Gateway migration

After updating your configuration:

1. Deploy the updated HPCC configuration
2. Check logs for authentication errors (should show Akeyless API responses, not Vault)
3. Verify secrets are retrieved correctly by running an ECL test job:

```bash
ecl run hthor examples/secrets/crypto_vault_secret.ecl
```

If you encounter issues, review the [Troubleshooting](#troubleshooting) section below.

--------------------------------------------------------------------------------------------------------

## Troubleshooting

- **"missing accessId for akeyless auth"** — The `accessId` property is missing or empty in the vault configuration.
- **"missing accessKey or client-secret for akeyless auth"** — Neither `accessKey` nor `client-secret` was provided.  One of the two is required.
- **"appRole secret `<name>` not found" or "appRole secret id not found at '`<name>`/secret-id'"** — When using the Akeyless HVP Gateway with Hashicorp AppRole-style configuration, ensure the secret referenced by `appRoleSecret` exists and its `secret-id` value is set to the Akeyless Access Key. Keep the same secret name, but replace the previous Vault secret-id value with the Akeyless Access Key.
- **Authentication failures (401/403)** — Verify that the access ID and access key are correct.  If using `client-secret`, verify the Kubernetes secret contains an `access-key` key with the correct value.  If using HVP with AppRole-style configuration, verify the secret referenced by `appRoleSecret` exists and that its `secret-id` value contains the Akeyless Access Key.
- **Secret not found (404)** — Verify that the full secret path (namespace + secret name) matches the path in Akeyless.  Check for leading slash mismatches.
- **Connection errors** — Verify the `url` is correct and reachable from within the cluster.  If using `verify_server: false`, ensure this is intentional (testing only).
