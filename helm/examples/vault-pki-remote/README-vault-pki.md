# Using a Hashicorp Vault PKI Certificate Authority to establish trust between two HPCC environments

This walkthough demonstrates using a single Hashicorp Vault PKI Certificate quthority to establish trust between two or more HPCC environments.

In the case of this example each HPCC environment is in a separate kubernetes namespace.

## Install hashicorp vault service in dev mode:

This is for development only, never deploy this way in production.
Deploying in dev mode sets up an in memory kv store that won't persist secret values across restart, and the vault will automatically be unsealed.

In dev mode the default root token is simply the string "root".

Add Hashicorp helm repo:

```bash
helm repo add hashicorp https://helm.releases.hashicorp.com
```

Update Helm repos.

```bash
helm repo update
```

Install vault server.

Note that a recent change to the developer mode vault means that you have to set the VAULT_DEV_LISTEN_ADDRESS environment variable as shown in order to access the vault service from an external pod.

```bash
helm install vault hashicorp/vault  --set "injector.enabled=false" --set "server.dev.enabled=true" --set 'server.extraEnvironmentVars.VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200' --namespace vaultns --create-namespace
```

Check the pods:
```bash
kubectl get pods -n vaultns
```

Vault pods should now be running and ready.


## Setting up vault

Tell the vault command line application the server location (dev mode is http, default location is https)

```bash
export VAULT_ADDR=http://127.0.0.1:8200
```

Export an environment variable for the vault CLI to authenticate with the Vault server.  Because we installed dev mode, the vault token is 'root'.

```bash
export VAULT_TOKEN=root
```

In a separate terminal window start vault port forwarding.

```bash
kubectl port-forward vault-0 8200:8200 -n vaultns
```

Login to the vault command line using the vault root token (development mode defaults to "root"):

```bash
vault login root
```

## Enable the PKI secrets engine at its default path.
```bash
vault secrets enable pki
```

Configure the max lease time-to-live (TTL) to 8760h.
```bash
vault secrets tune -max-lease-ttl=87600h pki
```

Generate the hpcc remote issuer CA, give it an issuer name.

```bash
vault write -field=certificate pki/root/generate/internal common_name="hpcc-issuer" issuer_name="hpcc-remote-issuer" ttl=87600h
```

Configure the PKI secrets engine certificate issuing and certificate revocation list (CRL) endpoints to use the Vault service in the "vaultns" namespace.

If you installed vault into a different namespace update the urls, replacing "vaultns" with the namespace used.

```bash
vault write pki/config/urls issuing_certificates="http://vault.vaultns:8200/v1/pki/ca" crl_distribution_points="http://vault.vaultns:8200/v1/pki/crl"
```

For our local MTLS certificates we will use our kubernetes namespace as our domain name. This will allow us to recongize where these components reside.
For our public TLS certificates for this demo we will use myhpcc.com as our domain.

Configure a role named hpccnamespace that enables the creation of certificates hpccnamespace domain with any subdomains.

```bash
vault write pki/roles/hpccremote key_type=any allowed_domains="hpcc1,hpcc2" allow_subdomains=true allowed_uri_sans="spiffe://*" max_ttl=72
```

Create a policy named pki that enables read access to the PKI secrets engine paths.

```bash
vault policy write hpcc-remote-pki - <<EOF
path "pki*"                   { capabilities = ["read", "list"] }
path "pki/roles/hpccremote"   { capabilities = ["create", "update"] }
path "pki/sign/hpccremote"    { capabilities = ["create", "update"] }
path "pki/issue/hpccremote"   { capabilities = ["create"] }
EOF
```

## Install cert-manager helm chart:

Add Jetstack helm repo:

```bash
helm repo add jetstack https://charts.jetstack.io
```

Install cert-manager.

```bash
helm install cert-manager jetstack/cert-manager --set installCRDs=true --namespace cert-manager --create-namespace
```


## Installing TWO HPCC environments that will be able to communicate in two separate namespaces


## For the first HPCC namespace "hpcc1"

```bash
kubectl create namespace hpcc1
```

The local and signing issuers are isolated and won't be using vault.  Create the secrets for this namespace.
For this kind of issuer the key pairs will be unique for every instance.

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout hpcc1local.key -sha256 -days 1825 -out hpcc1local.crt -config local-ca-req.cfg
kubectl create secret tls hpcc-local-issuer-key-pair --cert=hpcc1local.crt --key=hpcc1local.key -n hpcc1

openssl req -x509 -newkey rsa:2048 -nodes -keyout hpcc1signing.key -sha256 -days 1825 -out hpcc1signing.crt -config signing-ca-req.cfg
kubectl create secret tls hpcc-signing-issuer-key-pair --cert=hpcc1signing.crt --key=hpcc1signing.key -n hpcc1
```

The remote issuer does use vault.  Create the secret the remote issuer that hpcc1 will use to access the vault pki engine

```bash
kubectl create secret generic cert-manager-vault-token --from-literal=token=root -n hpcc1
```

```bash
helm install myhpcc hpcc/hpcc --values values-hpcc1.yaml -n hpcc1
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods -n hpcc1
```

Check and see if the cerficate issuers have been successfully created.


## Repeat for the second HPCC namespace "hpcc2"

```bash
kubectl create namespace hpcc2
```

The local and signing issuers are isolated and won't be using vault.  Create the secrets for this namespace.
For this kind of issuer the key pairs will be unique for every instance.

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout hpcc2local.key -sha256 -days 1825 -out hpcc2local.crt -config local-ca-req.cfg
kubectl create secret tls hpcc-local-issuer-key-pair --cert=hpcc2local.crt --key=hpcc2local.key -n hpcc2

openssl req -x509 -newkey rsa:2048 -nodes -keyout hpcc2signing.key -sha256 -days 1825 -out hpcc2signing.crt -config signing-ca-req.cfg
kubectl create secret tls hpcc-signing-issuer-key-pair --cert=hpcc2signing.crt --key=hpcc2signing.key -n hpcc2
```

The remote issuer does use vault.  Create the secret the remote issuer that hpcc1 will use to access the vault pki engine

```bash
kubectl create secret generic cert-manager-vault-token --from-literal=token=root -n hpcc2
```

```bash
helm install myhpcc hpcc/hpcc --values values-hpcc2.yaml -n hpcc2
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods -n hpcc2
```

Check and see if the cerficate issuers have been successfully created.

## ECL example demonstrating trust

Now we can run some ECL in each environment that will talk to each other.

roxie_echo.ecl which returns a dataset passed into it.
remote_echo.ecl which calls roxie_echo.ecl.

For this example we will:
1. publish roxie_echo.ecl to the hpcc1 namespace.
2. Publish remote_echo.ecl to the hpcc2 namespace.
3. Use hpcc2::remote_echo.ecl to call hpcc1::roxie_echo.ecl.

## Publish the queries:

```bash
ecl publish roxie1 --ssl --port 18010 roxie_echo.ecl
ecl publish roxie2 --ssl --port 28010 remote_echo.ecl
```

## Call the query and demonstrate trust

You can navigate to EclQueries/WsEcl on port 28002 in your browser and run the "remote_roxie"
query from there, or you can use curl from the command line as shown below.

NOTE: The use of --insecure for the curl command line has nothing to do with the trust between environments.
It only reflects the way this demo set up the one service EclQqueries.  The two roxies are using
Hashicorp Vault PKI to secure communications.  For the purpose of this walkthrough on the other hand
EclQueries is set up using self signed certificates, which should never be done in a production
environment.

```bash
curl https://localhost:28002/WsEcl/submit/query/roxie2/remote_echo/json --insecure
```

Example Output:

{"remote_echoResponse":  {"sequence": 0, "Results":  {"remoteResult": {"Row": [{"Dataset": {"Row": [{"name": {"first": "Joeseph", "last": "Johnson"}, "address": {"city": "Fresno", "state": "CA", "zipcode": "11111"}}, {"name": {"first": "Joeseph", "last": "Johnson"}, "address": {"city": "Fresno", "state": "CA", "zipcode": "22222"}}, {"name": {"first": "Joeseph", "last": "Johnson"}, "address": {"city": "Fresno", "state": "CA", "zipcode": "33333"}}, {"name": {"first": "Joeseph", "last": "Johnson"}, "address": {"city": "Fresno", "state": "CA", "zipcode": "44444"}}, {"name": {"first": "Joeseph", "last": "Johnson"}, "address": {"city": "Fresno", "state": "CA", "zipcode": "55555"}}]}, "Exception": {"Code": "0"}}]}}}}
