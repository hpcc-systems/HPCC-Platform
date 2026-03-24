# Remote Storage Using Azure API

This document describes how to configure a bare-metal Azure-VM HPCC Systems environment to access remote cloud storage via the Azure Storage API. This is the recommended approach for copying files from a cloud-based HPCC environment (e.g., a production Thor cluster running in Azure) to a local HPCC environment.

For background on why Azure API copying is preferred over traditional dafilesrv-based transfers, see [Remote File Roxie](Remote%20File%20Roxie.md).

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

## Setting Up Authentication

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
