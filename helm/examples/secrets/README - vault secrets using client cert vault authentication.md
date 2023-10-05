# Containerized HPCC Systems Vault Secrets using appRole vault authentication

This example demonstrates HPCC use use of Hashicorp Vault secrets using the "TLS certificates auth method".

This example assumes you are starting from a linux command shell in the HPCC-Platform/helm directory.  From there you will find the example files and this README file in the examples/secrets directory.

## Hashicorp Vault support:

This example uses Hashicorp vault.  The following steps can be used to set up a development mode only instance of vault just for the purposes of this example.  This makes it easy to test out vault functionality without going through the much more extensive configuration process for a production ready vault installation.

## Install hashicorp vault command line client on your local system:

https://learn.hashicorp.com/tutorials/vault/getting-started-install

--------------------------------------------------------------------------------------------------------

## Install cert-manager

Install cert-manager support following the example found at examples/certmanager.

With the addition of the following step:

Repeat the root certificate creation process from the cert-manager setup to create the CA secret for the vaultclient issuer.

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout vaultclientca.key -sha256 -days 1825 -out vaultclientca.crt -config examples/certmanager/ca-req.cfg
```

## Create a Kubernetes TLS secret from the generated signing root certificate and privatekey

```bash
kubectl create secret tls hpcc-vaultclient-issuer-key-pair --cert=vaultclientca.crt --key=vaultclientca.key
```


## Install hashicorp vault service in standalone mode with tls:

Install vault server with tls support.  This is currently outside the scope of this document.


## Setting up vault

Tell the vault command line application the server location (dev mode is http, default location is https)

```bash
export VAULT_ADDR=https://127.0.0.1:8200
```

In a separate terminal window start vault port forwarding.

```bash
kubectl port-forward vault-0 8200:8200
```

Login to the vault command line using the vault root token (development mode defaults to "root"):

```bash
vault login <token>
```

If you don't provide the token on the command line you will be prompted to input the value and it will be hidden from view.


## Configure vault tls cert auth

Enabling appRole auth will allow access the vault via the appRole authentication protocol.

```bash
vault auth enable cert
```

Setup vault auth policy granting access to the ecl secrets locations we plan to use:

```bash
vault policy write hpcc-ecl-ro examples/secrets/hpcc_vault_ecl_policies.hcl
```

Setup hpcc-vault-access auth role:

```bash
vault write auth/cert/certs/hpcc-ecl \
    display_name=hpcc-ecl-cert \
    policies=hpcc-ecl-ro \
    certificate=@vaultclientca.crt \
    allowed_common_names=ecl.vaultclient.hpcc.example.com \
    ttl=3600
```

Setup vault auth policy granting access to the eclUser secrets locations we plan to use:

```bash
vault policy write hpcc-ecluser-ro examples/secrets/hpcc_vault_ecluser_policies.hcl
```

Setup hpcc-vault-access auth role:

```bash
vault write auth/cert/certs/hpcc-ecluser \
    display_name=hpcc-ecluser-cert \
    policies=hpcc-ecluser-ro \
    certificate=@vaultclientca.crt \
    allowed_common_names=ecluser.vaultclient.hpcc.example.com \
    ttl=3600
```


## 'eclUser' category secrets

Create example vault 'eclUser' secrets:

```bash
vault kv put secret/eclUser/vault-example crypt.key=@examples/secrets/crypt.key
```

## 'ecl' category secrets

Secrets in the 'ecl' category are not accessible by ECL code directly and therefore not visible to ECL users.  They can be used by internal ECL feartures
and commands.  For example:

## HTTP-CONNECT Secrets:

This example focuses on ECL secrets to provide HTTP connection strings and credentials for ECL SOAPCALL and HTTPCALL commands.

These secrets are prefixed with the string "http-connect-" requiring this prefix ensures that HTTPCALL/SOAPCALL only accesses secrets which are intended for this use.

HTTP-CONNECT secrets consist of a url string and optional additional secrets associated with that URL.  Requiring the url to be part of the secret prevents credentials from being easily hijacked via an HTTPCALL to an arbitrary location.  Instead the credentials are explicitly associated with the provided url.

Besides the URL values can currently be set for proxy (trusted for keeping these secrets), username, and password.

## Creating the HTTP-CONNECT Secrets

## Create example vault secret:

Create example vault 'ecl' secrets:

```bash
vault kv put secret/ecl/http-connect-vaultsecret url=@examples/secrets/url-basic username=@examples/secrets/username password=@examples/secrets/password
```

## Installing the HPCC with the secrets added to ECL components

Install the HPCC helm chart with the secrets just defined added to all components that run ECL.

```bash
helm install myhpcc hpcc/ -f examples/secrets/values-secrets-client-cert-auth.yaml
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods
```
--------------------------------------------------------------------------------------------------------

If you don't already have the HPCC client tools installed please install them now:

https://hpccsystems.com/download#HPCC-Platform


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

If you don't already have the HPCC client tools installed please install them now:

https://hpccsystems.com/download#HPCC-Platform

--------------------------------------------------------------------------------------------------------

The following ecl command will run the example ECL file that demonstrates an HTTPCALL that uses a vault secret for connection and  authentication.

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
