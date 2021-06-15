# Containerized HPCC LDAP Support

These examples demonstrate how to enable the HPCC LDAP Security Manager, by adding LDAP administrator account credentials directly to the HPCC helm charts, or by externalizing these credentials using Kubernetes and Hashicorp Vault secrets. Note there are many variations on how to set these values, using yaml, override files and command line settings. To use externalized credentials, you should first run the tutorial on setting up Kubernetes secrets and the Hashicorp Vault, found in the README.md file in the "HPCC-Platform\helm\examples\secrets" folder.

Note that the LDAP Administrator account performs AD directory searches and modifications, and is the only HPCC user that must have LDAP administrator rights.  This account should exist in the configured "systemBasedn" branch of the LDAP AD directory, typically set to cn=Users.

--------------------------------------------------------------------------------------------------------

## Configure LDAP using helm charts and yaml

### Add credentials to ECLWatch ldap.yaml
   Edit the ECLWatch HPCC-Platform\esp\applications\common\ldap\ldap.yaml (or override file), and fill out the systemCommonName, systemUser, and MD5 encrypted systemPassword entries as follows.
   Ensure the other settings are correct for your directory, especially the OU settings. Make sure that ldapAdminSecretKey and ldapAdminVaultId are empty.

```bash
   ldapAdminSecretKey: ""
   ldapAdminVaultId: ""
   systemCommonName: "hpcc_admin"
   systemPassword: "6b3f606902cb9a337904efe2650eb771"
   systemUser: "hpcc_admin"
```

### Enable LDAP authentication on the ECLWatch command line
   Add the following "auth" and "ldapAddress" settings to your ECLWatch command line or override file, ensuring the ldapAddress is the IP of your LDAP server.

```bash
esp --application=eclwatch --auth=ldap --ldapAddress=10.200.1.2 ...
```

--------------------------------------------------------------------------------------------------------
## Configure LDAP to use externalized Kubernetes (k8s) secrets

### Create the k8s secret
   From the CLI, create the LDAP "secret" similar to the following.
   Make note of the secret name, "myAdminCreds" in this example.
   The "username" and "password" key/values are required, additional properties are allowed but ignored.

```bash
   kubectl create secret generic myAdminCreds --from-literal=username=hpcc_admin --from-literal=password=t0pS3cr3tP@ssw0rd
   kubectl get secret myAdminCreds
```
For more details on how to create secrets, see the "secrets" examples in the "HPCC-Platform\helm\examples\secrets" folder.

### Deploy the secret to the ECLWatch container
   Modify the HPCC-Platform\helm\hpcc\values.yaml or override file "secrets: / authn:" category as follows.
   Create a unique key name ("ldapadminsecretkey01" in this example), and set it to the secret value ("myAdminCreds") that you created above.

```bash
   secrets:
     authn:
       ldapadminsecretkey01: "myAdminCreds"
```

### Reference the k8s secret key
   In the HPCC-Platform\esp\applications\common\ldap\ldap.yaml or override, set the "ldapAdminSecretKey" to the key name created above. Note that the key name "ldapadminsecretkey" is used by the LDAP security manager to resolve the secret name, and must be spelled exactly as follows.

```bash
   ldapAdminSecretKey: "ldapadminsecretkey01"
   ldapAdminVaultId: ""
   systemCommonName: ""
   systemPassword: ""
   systemUser: ""
```

### Enable LDAP authentication on the ECLWatch command line
   Add the following "auth" and "ldapAddress" settings to your ECLWatch command line or override file, ensuring the ldapAddress is the IP of your LDAP server.

```bash
esp --application=eclwatch --auth=ldap --ldapAddress=10.200.1.2 ...
```

--------------------------------------------------------------------------------------------------------
## Configure LDAP to use externalized Hashicorp Vault secrets

### Create the vault secret
   From the CLI, create the LDAP vault "secret" similar to the following.
   Make note of the secret name, "myVaultAdminCreds" in this example.
   The "username" and "password" key/values are required, additional properties are allowed but ignored.
   Make sure the secret name is specified with the "secret/authn/" prefix

```bash
   vault kv put secret/authn/myVaultAdminCreds username=hpcc_admin password=t0pS3cr3tP@ssw0rd
   vault kv get secret/authn/myVaultAdminCreds
```

   For more details on how to create vault secrets, see the "secrets" examples in the "HPCC-Platform\helm\examples\secrets" folder.

### Note that the vault name, my-authn-vault, was defined in the "secrets" tutorial, in the HPCC-Platform\helm\examples\secrets\values-secrets.yaml file as follows

```bash
  authn:
    - name: my-authn-vault
      #Note the data node in the URL is there for the REST APIs use. The path inside the vault starts after /data
      url: http://${env.VAULT_SERVICE_HOST}:${env.VAULT_SERVICE_PORT}/v1/secret/data/authn/${secret}
      kind: kv-v2
```

### Reference the secret key in the LDAP yaml or override
   The key names "ldapadminsecretkey" and "ldapAdminVaultId" are used by the LDAP security manager to resolve the secret, and must be spelled exactly as follows.

```bash
    ldapadminsecretkey: "myVaultAdminCreds"
    ldapAdminVaultId: "my-authn-vault"
```

### Enable LDAP authentication on the ECLWatch command line
   Add the following "auth" and "ldapAddress" settings to your ECLWatch command line or override file, ensuring the ldapAddress is the IP of your LDAP server.

```bash
esp --application=eclwatch --auth=ldap --ldapAddress=10.200.1.2 ...
```