# Containerized HPCC Systems Secrets

This example demonstrates HPCC use use of Kubernetes and Hashicorp Vault secrets.

This example assumes you are starting from a linux command shell in the HPCC-Platform/helm directory.  From there you will find the example files and this README file in the examples/secrets directory.

## Hashicorp Vault support:

This example uses Hashicorp vault.  The following steps can be used to set up a development mode only instance of vault just for the purposes of this example.  This makes it easy to test out vault functionality without going through the much more extensive configuration process for a production ready vault installation.

## Install hashicorp vault command line client:

https://learn.hashicorp.com/tutorials/vault/getting-started-install

--------------------------------------------------------------------------------------------------------

## Install hashicorp vault service in dev mode:

This is for development only, never deploy this way in production.
Deploying in dev mode sets up an in memory kv store that won't persist secret values across restart, and the vault will automatically be unsealed.

In dev mode the default root token is simply the string "root".

Add Hashicorp helm repo:

```bash
helm repo add hashicorp https://helm.releases.hashicorp.com
```

Install vault server.

Note that a recent change to the developer mode vault means that you have to set the VAULT_DEV_LISTEN_ADDRESS environment variable as shown in order
to access the vault service from an external pod.

```bash
helm install vault hashicorp/vault --set "server.dev.enabled=true" --set 'server.extraEnvironmentVars.VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200'
```

## Setting up vault

Tell the vault command line application the server location (dev mode is http, default location is https)

```bash
export VAULT_ADDR=http://127.0.0.1:8200
```

In a separate terminal window start vault port forwarding.

```bash
kubectl port-forward vault-0 8200:8200
```

Login to the vault command line using the vault root token (development mode defaults to "root"):

```bash
vault login root
```

If you don't provide the token on the command line you will be prompted to input the value and it will be hidden from view.


## Configure vault kubernetes auth

Enabling kubernetes auth will allow k8s nodes to access the vault via their kubernetes.io access tokens.

```bash
vault auth enable kubernetes
```

Exec into the vault-0 pod:

```bash
kubectl exec -it vault-0 /bin/sh
```

Configure vault kubernetes auth:

```bash
vault write auth/kubernetes/config \
   token_reviewer_jwt="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
   kubernetes_host=https://${KUBERNETES_PORT_443_TCP_ADDR}:443 \
   kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
```

Exit from the vault-0 pod:

```bash
exit
```

Setup vault auth policy granting access to the ecl secrets locations we plan to use:

```bash
vault policy write hpcc-kv-ro examples/secrets/hpcc_vault_policies.hcl
```

Setup hpcc-vault-access auth role within the default service account (if necessary change bound_service_account "names" and "namespace" to match the service account the HPCC deployment is using):

```bash
vault write auth/kubernetes/role/hpcc-vault-access \
        bound_service_account_names="*" \
        bound_service_account_namespaces=default \
        policies=hpcc-kv-ro \
        ttl=24h
```

## HTTP-CONNECT Secrets:

This example focuses on ECL secrets to provide HTTP connection strings and credentials for ECL SOAPCALL and HTTPCALL commands.

These secrets are prefixed with the string "http-connect-" requiring this prefix ensures that HTTPCALL/SOAPCALL only accesses secrets which are intended for this use.

HTTP-CONNECT secrets consist of a url string and optional additional secrets associated with that URL.  Requiring the url to be part of the secret prevents credentials from being easily hijacked via an HTTPCALL to an arbitrary location.  Instead the credentials are explicitly associated with the provided url.

Besides the URL values can currently be set for proxy (trusted for keeping these secrets), username, and password.

## Creating the HTTP-CONNECT Secrets

## Create example vault secret:

Create example vault secrets:

```bash
vault kv put secret/ecl/http-connect-vaultsecret url=@examples/secrets/url-basic username=@examples/secrets/username password=@examples/secrets/password
```

The following vault secret will be hidden by our "local" kubernetes secret below by default.  But we can ask for it directly in our HTTPCALL (see "httpcall_vault.ecl" example).

```bash
vault kv put secret/ecl/http-connect-basicsecret url=@examples/secrets/url-basic username=@examples/secrets/username password=@examples/secrets/password
```

Create example kubernetes secret:

```bash
kubectl create secret generic http-connect-basicsecret --from-file=url=examples/secrets/url-basic --from-file=examples/secrets/username --from-file=examples/secrets/password
```

## Installing the HPCC with the HTTP-CONNECT Secrets added to ECL components

Install the HPCC helm chart with the secrets just defined added to all components that run ECL.

```bash
helm install myhpcc hpcc/ --set global.image.version=latest -f examples/secrets/values-secrets.yaml
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods
```

## Using the created secrets via HTTPCALL from within ECL code

If you don't already have the HPCC client tools installed please install them now:

https://hpccsystems.com/download#HPCC-Platform

--------------------------------------------------------------------------------------------------------

The following ecl commands will run the three example ECL files on hthor.

```bash
ecl run hthor examples/secrets/httpcall_secret.ecl

ecl run hthor examples/secrets/httpcall_vault.ecl

ecl run hthor examples/secrets/httpcall_vault_direct.ecl
```

For each job the expected result would be:

```xml
<Result>
<Dataset name='Result 1'>
 <Row><authenticated>true</authenticated></Row>
</Dataset>
</Result>
```
