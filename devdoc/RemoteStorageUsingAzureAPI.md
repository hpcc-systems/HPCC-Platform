# Remote Storage Using Azure API

This document describes how to configure a bare-metal Azure-VM HPCC Systems environment to access remote cloud storage via the Azure Storage API. This is the recommended approach for copying files from a cloud-based HPCC environment (e.g., a production Thor cluster running in Azure) to a local HPCC environment.

## Prerequisites

Before starting, ensure the following are in place:

- **Source environment**: A running HPCC cluster in Azure with data stored in Azure Storage and a DFS (Distributed File Service) endpoint exposed.
- **Target environment**: A bare-metal or VM system where HPCC will be deployed with Azure Storage mounted via NFS.
- **Network peering**: The target VM must be able to reach the remote Azure environment's DFS endpoint. Coordinate with HPCC OPS to ensure the target system's IP range is peered with the remote storage network.
- **TLS certificates**: You will need `tls.key`, `ca.crt`, and `tls.crt` for the remote DFS service. These can be extracted from the Kubernetes secret backing the DFS service in the source environment. See this [documentation JIRA](https://hpccsystems.atlassian.net/browse/HPCC-27688?focusedCommentId=47453) for more details on generating keys.
- **Azure Managed Identity**: The target VM must have a managed identity (system-assigned or user-assigned) that is authorized to access the required Azure storage resources. This is used for authentication — no SAS tokens or storage keys are needed. The identity must be granted the following roles:

  | Resource | Required Role | Purpose |
  |---|---|---|
  | Remote Azure Blob Storage accounts | **Storage Blob Data Reader** | Read file data from the remote Thor environment's blob containers via the Azure Blob API. |
  | Remote Azure Blob Storage accounts | **Storage Blob Data Contributor** | Required only if the target system needs to write back to remote blob storage |
  | Local Azure Files storage account | **Storage File Data Privileged Contributor** | Required to copy data to Azure Files Mount. |

## Configuring Remote Storage in environment.xml

The core of the setup is adding remote storage plane definitions to the target system's `environment.xml`. This tells the local HPCC system how to find and access files on the remote Azure Blob Storage.

### Add Storage Plane and Remote Service Definitions

In the `<Globals>` section of `environment.xml` add your storage plane definitions. The example below shows:

- **`plane1`**: A remote storage plane pointing to Azure Blob Storage. The `numDevices` and number of `<containers>` entries must match the source environment's configuration. Each container entry maps to a separate Azure storage account.
- **`roxie_01`**: A local data plane backed by Azure File storage, used as the destination for file copies.
- **`plane1-data`**: A remote service definition that connects the local plane name to the remote DFS endpoint.

```xml
<Globals>
    <storage>
      <planes category="remote"
              name="plane1"
              numDevices="20"
              striped="1"
              prefix="azureblob:plane1">
              <storageapi type="azureblob" managed="1">
                  <containers account="<STORAGE_ACCOUNT_1>" name="hpcc-data"/>
                  <containers account="<STORAGE_ACCOUNT_2>" name="hpcc-data"/>
                  <!-- ... one <containers> entry per device, matching numDevices -->
                  <containers account="<STORAGE_ACCOUNT_N>" name="hpcc-data"/>
              </storageapi>
      </planes>
      <planes name="roxie_01"
              category="data"
              defaultSprayParts="400">
              <storageapi managed="1"
                          type="azurefile">
                          <containers name="data" account="<LOCAL_STORAGE_ACCOUNT>"/>
              </storageapi>
      </planes>
      <remote name="plane1-data"
              service="https://<DFS_ENDPOINT>">
              <planes remote="data"
                      local="plane1"/>
      </remote>
    </storage>
  </Globals>
```

Key points about this configuration:

- The plane's `category` must be `remote` (not `data`).
- The `managed="1"` attribute on `<storageapi>` tells HPCC to use Azure Managed Identity for authentication rather than storage keys or SAS tokens.
- The `<remote>` element's `service` URL must point to the DFS service of the source environment. You can find this URL by inspecting the Kubernetes service or pod YAML in the source cluster.
- The `<planes>` element within `<remote>` maps `remote="data"` (data is the plane name on the source side) to `local="plane1"` (the plane name on the target side).
- The `striped=1` is only necessary if reading from striped storage on a bare-metal cluster

## Azure Blob Write Settings

Azure Blob write settings and runtime behavior are shared with containerized
deployments. For defaults, limits, precedence, resource estimates, and write
semantics, see [Azure Blob API writes in the Helm storage documentation](../helm/hpcc/docs/storage.md#azure-blob-api-writes).

On bare metal, configure the same settings in `environment.xml`. Global
defaults go under `<Globals><expert><storageapi .../></expert></Globals>`;
per-plane overrides go under the plane's `<storageapi><expert .../></storageapi>`.
For example, merge these settings into the existing global configuration and
desired plane definition, preserving the plane's other attributes and storage
API settings:

```xml
<Globals>
    <expert>
        <storageapi parallelWriteConcurrency="16"/>
    </expert>
    <storage>
        <planes name="plane1">
            <storageapi>
                <expert parallelWriteConcurrency="0"/>
            </storageapi>
        </planes>
    </storage>
</Globals>
```

### Optional Write Tests

`AzureBlobWriteExternalServiceTest` checks functional write/readback and directory
traversal. `AzureBlobWriteStressTimingTest` measures write throughput at concurrency
levels 0, 1, 2, 4, 8, 16 and 32, and verifies readback. Both suites make real requests
and are excluded from the default test run.

Readback uses 4 MiB chunks: `createSerialInputStream` at concurrency 0 and 1, and
`createParallelReadAheadInputStream` with the matching thread count at concurrency
2 and above. Read throughput is reported separately from write throughput; its
wall-clock timing includes opening the reader, read-ahead setup, consumption and
content verification.

Both accept these suite-prefixed options:

| Option | Meaning |
|---|---|
| `url` | Encoded HTTP(S) base blob URL, including a container and blob-name prefix; no SAS query or fragment |
| `auth` | `sharedKey` (default) or `managedIdentity` |
| `accountName` | Required for explicitly configured Azurite/custom hosts; otherwise derived from standard Azure blob hosts |

Shared-key authentication reads `HPCC_AZURE_TEST_ACCOUNT_KEY`. The older
`HPCC_AZURE_WRITE_STRESS_ACCOUNT_KEY` variable remains supported when the new
variable is unset. With no URL option, both suites use local Azurite
at `127.0.0.1:10000` with its standard development account and key.

Each suite appends unique suffixes to the supplied base URL and deletes its blobs
after successful verification; it does not overwrite the base blob itself.
Failures can leave test blobs behind. Both suites create the container
if needed and do not delete it.
The functional suite retains the `127.0.0.1` to `localhost` alias check only for
loopback endpoints; Azure hostnames are left unchanged.

Set these paths for your build or installation:

```bash
UT=/path/to/bin/unittests
HOOKS=/path/to/filehooks
```

`HOOKS` may also name the built `libazurefile.so` directly.
Omit `-d "$HOOKS"` when the runner automatically discovers the Azure test library.

#### Running against Azurite

Start an installed Azurite Blob service in a separate terminal:

```bash
azurite-blob --location /tmp/hpcc-azurite-tests --blobHost 127.0.0.1 --blobPort 10000
```

Neither suite needs connection options:

```bash
"$UT" -v -d "$HOOKS" -e AzureBlobWriteExternalServiceTest

"$UT" -v -d "$HOOKS" -e AzureBlobWriteStressTimingTest \
    --AzureBlobWriteStressTimingTest.sizeMB=16
```

To configure a different Azurite endpoint for either suite, supply its
suite-prefixed `url` and `accountName` options and set `HPCC_AZURE_TEST_ACCOUNT_KEY`
to the Azurite account key. Direct-URL factories use host-style URLs by default;
set the optional `pathStyle` argument to `true` for path-style URLs. The external
test suites set it automatically for their built-in local Azurite endpoint. For
a path-style Azurite endpoint accessed through a custom DNS name, set the suite's
`pathStyle=true` option. For example:

```text
--AzureBlobWriteExternalServiceTest.url=http://azurite:10000/devstoreaccount1/hpccparalleltest/azure-blob-test
--AzureBlobWriteExternalServiceTest.accountName=devstoreaccount1
--AzureBlobWriteExternalServiceTest.pathStyle=true
```

#### Running against Azure Storage

Use an account that permits shared-key authentication, or an available managed
identity with read, write, list and delete access (typically **Storage Blob Data
Contributor**). Both suites also need permission to create the container.
Supply a disposable blob-name prefix in the target container.

For shared-key authentication, read the key without putting it in shell history:

```bash
read -rsp 'Azure storage account key: ' HPCC_AZURE_TEST_ACCOUNT_KEY
echo
export HPCC_AZURE_TEST_ACCOUNT_KEY

"$UT" -v -d "$HOOKS" -e AzureBlobWriteExternalServiceTest \
    --AzureBlobWriteExternalServiceTest.url=https://ACCOUNT.blob.core.windows.net/CONTAINER/functional

"$UT" -v -d "$HOOKS" -e AzureBlobWriteStressTimingTest \
    --AzureBlobWriteStressTimingTest.url=https://ACCOUNT.blob.core.windows.net/CONTAINER/stress \
    --AzureBlobWriteStressTimingTest.sizeMB=64
```

Replace `ACCOUNT` and `CONTAINER` with your storage account and container.
For managed identity, omit the key and add the corresponding suite's
`--AzureBlobWriteExternalServiceTest.auth=managedIdentity` or
`--AzureBlobWriteStressTimingTest.auth=managedIdentity` option. Run on a machine or
pod with a configured Azure managed/workload identity; an Azure CLI login alone
does not configure this authentication mode.

The stress suite defaults to 1024 MiB **per concurrency level** and reads every
blob back, so use a smaller `sizeMB` for a quick check or to limit cloud traffic.

## Setting Up Authentication

### Internal Direct-URL Factories

The `createAzureBlob` factories are internal to the `azurefile` library and its
tests. They are intentionally not exported or supported for external C++ callers.
External callers should use `createIFile()` with the installed Azure file hooks.

These internal factories support encoded HTTP(S) blob URLs without a storage
plane. `createAzureBlob(blobUrl, accountName, accountKey)` uses a shared key;
`createAzureBlob(blobUrl, accountName)` uses the configured managed identity.
The managed-identity overload also accepts a `StorageApiConfig` argument.
Managed identity must be enabled in the environment. These URL factories reject
SAS query strings and URL fragments. Container URLs can be used for directory
creation and enumeration.

### Verify Network Peering and Keys

The target system's network must be peered with the remote Azure environment. Confirm that you can reach the DFS endpoint from the target system using the generated keys:

```bash
curl -v --cacert ca.crt --cert tls.crt --key tls.key https://<DFS-ENDPOINT>/alive
```

### Verify Azure Managed Identity

If using Managed Identity, the target VM must have an Azure Managed Identity that is authorized to access the remote blob storage accounts. Verify that the identity is available by requesting a token from the Azure Instance Metadata Service (IMDS):

```bash
curl -s -H "Metadata: true" \
  "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/" \
  --connect-timeout 2
```

A successful response will include an `access_token`, `client_id`, `expires_on`, and other fields:

```json
{
  "access_token": "<TOKEN>",
  "client_id": "<CLIENT_ID>",
  "expires_in": "86189",
  "expires_on": "...",
  "ext_expires_in": "86399",
  "not_before": "...",
  "resource": "https://management.azure.com/",
  "token_type": "Bearer"
}
```

If the `access_token` is empty or the request times out, the VM does not have a managed identity configured. Work with your Azure administrator to assign one.

### Copy Files with dfuplus

Use `dfuplus` to copy a file from the remote plane to a local plane:

```bash
dfuplus server=. action=copy \
  srcname=~remote::plane1-data::<LOGICAL_FILE_NAME> \
  dstname=<DESTINATION_FILE_NAME> \
  dstcluster=roxie_01 \
  overwrite=1 \
  compress=0 \
  replicate=0 \
  username=<USERNAME>
```

- `server=.` directs `dfuplus` to the local ESP server.
- The `srcname` uses the `remote::<remote-plane-name>::` prefix to reference a file on the source environment.
- `dstcluster` specifies the local storage plane to copy into.

