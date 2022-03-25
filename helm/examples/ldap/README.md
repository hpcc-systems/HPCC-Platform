# Containerized HPCC LDAP Support

These examples demonstrate how to externalize HPCC LDAP Active Directory Security Manager administrator account credentials using Kubernetes and Hashicorp Vault secrets. To use externalized credentials, you should first run the tutorial on setting up Kubernetes secrets and the Hashicorp Vault, which can be found in the README.md file in the "HPCC-Platform/helm/examples/secrets" folder.

Note that the LDAP Administrator account performs AD directory searches and modifications, and is the only HPCC user that must have Active Directory administrator rights.  This account should exist in the configured "systemBasedn" branch of the Active Directory, typically set to cn=Users.

--------------------------------------------------------------------------------------------------------
## Configure LDAP to use externalized Kubernetes (k8s) secrets

### Create the k8s secret
   From the CLI, create the LDAP "secret" similar to the following.
   Make note of the secret name, "admincredssecretname" in this example.
   The LDAP Administrator "username" and "password" key/values are required; additional properties are allowed but ignored.

```bash
   kubectl create secret generic admincredssecretname --from-literal=username=hpcc_admin --from-literal=password=t0pS3cr3tP@ssw0rd
   kubectl get secret admincredssecretname
```
For more details on how to create secrets, see the "secrets" examples in the "HPCC-Platform/helm/examples/secrets" folder.

### Deploy the k8s secret to the ECLWatch container
   Override the HPCC-Platform/helm/hpcc/values.yaml's "secrets:" category as follows.
   Create a unique key name used to reference the secret (this will be the mounted file system name of the secret), and set it to the secret value ("admincredssecretname") that you created above. In this example we give the key the name "admincredsmountname," and optionally define an additional alternate one "admincredsaltmountname" which could be used with another Active Directory server.
   Note that the "admincredsmountname" key/value pair already exists as a default in the values.yaml file, and the key is referenced in the component's ldap.yaml file.  You may override these and add additional key/values as needed.

```bash
   secrets:
     authn:
       admincredsmountname: "admincredssecretname"       #exernalize LDAP Admin creds
       admincredsaltmountname: "admincredsaltsecretname" #exernalize alternate LDAP Admin creds
```

### Enable LDAP and reference the k8s secret key
   In the HPCC-Platform/esp/applications/common/ldap/ldap.yaml file, the "ldapAdminSecretKey" is already set to the key mount name created above. To enable LDAP authentication and to optionally override this value, override the ESP/ECLWatch helm component located in values.yaml as follows.

```bash
esp:
- name: eclwatch
  application: eclwatch
  auth: ldap
  ldap:
    ldapAddress: "myldapserver"
    ldapAdminSecretKey: "admincredsaltmountname"   # use alternate secrets creds
```

--------------------------------------------------------------------------------------------------------
## Configure LDAP to use externalized Hashicorp Vault secrets

### Create the vault secret
   From the CLI, create the LDAP vault "secret" similar to the following.
   Make note of the secret name, "myvaultadmincreds" in this example.
   The LDAP Administrator "username" and "password" key/values are required; additional properties are allowed but ignored.
   Make sure the secret name is specified with the "secret/authn/" prefix.

```bash
   vault kv put secret/authn/myvaultadmincreds username=hpcc_admin password=t0pS3cr3tP@ssw0rd
   vault kv get secret/authn/myvaultadmincreds
```

   For more details on how to create vault secrets, see the "secrets" examples in the "HPCC-Platform/helm/examples/secrets" folder.

### Note that the vault name, my-authn-vault, was defined in the "secrets" tutorial, in the HPCC-Platform/helm/examples/secrets/values-secrets.yaml file as follows

```bash
vaults:
  authn:
    - name: my-authn-vault
      #Note the data node in the URL is there for the REST APIs use. The path inside the vault starts after /data
      url: http://${env.VAULT_SERVICE_HOST}:${env.VAULT_SERVICE_PORT}/v1/secret/data/authn/${secret}
      kind: kv-v2
```

### Reference the secret key in the LDAP yaml or override
   The key names "ldapAdminSecretKey" and "ldapAdminVaultId" are used by the LDAP security manager to resolve the secret, and must be spelled exactly as follows.

```bash
esp:
- name: eclwatch
  application: eclwatch
  auth: ldap
  ldap:
    ldapAddress: "myldapserver"
    ldapAdminSecretKey: "myvaultadmincreds"
    ldapAdminVaultId: "my-authn-vault"
```