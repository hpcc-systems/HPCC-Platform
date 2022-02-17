# Install Hashicorp Vault

See also https://learn.hashicorp.com/tutorials/vault/kubernetes-cert-manager for a more Vault centric tutorial on setting up cert-manager with vault.

## Add hashicorp to you helm repo
```bash
helm repo add hashicorp https://helm.releases.hashicorp.com
```

## Helm install hashicorp vault

Disable the vault sidecar injector by setting "injector.enabled=false".

```bash
helm install vault hashicorp/vault --set "injector.enabled=false"
```

Check the pods:
```bash
kubectl get pods
```

Vault pods should be running, but not ready

```bash
$ kubectl get pods
NAME                                       READY   STATUS    RESTARTS   AGE
vault-0                                    0/1     Running   0          6s
```

## Initialize and unseal the vault

Initialize Vault with one key share and one key threshold.  Saving off the output in json format so
we can utilize the unseal key and root token later.

```bash
kubectl exec vault-0 -- vault operator init -key-shares=1 -key-threshold=1 -format=json > init-keys.json
```

View the unseal key found in init-keys.json.

```bash
cat init-keys.json | jq -r ".unseal_keys_b64[]"
```

Create an environment variable holding the unseal key:

```bash
VAULT_UNSEAL_KEY=$(cat init-keys.json | jq -r ".unseal_keys_b64[]")
```

Unseal Vault running on the vault-0 pod with the $VAULT_UNSEAL_KEY.

```bash
kubectl exec vault-0 -- vault operator unseal $VAULT_UNSEAL_KEY
```

Check the pods:
```bash
kubectl get pods
```

Vault pods should now be running and ready.

## Configure the Vault PKI secrets engine (certificate authority)

View the vault root token:
```bash
cat init-keys.json | jq -r ".root_token"
```

Create a variable named VAULT_ROOT_TOKEN to capture the root token.
```bash
VAULT_ROOT_TOKEN=$(cat init-keys.json | jq -r ".root_token")
```

Login to Vault running on the vault-0 pod with the $VAULT_ROOT_TOKEN.
```bash
kubectl exec vault-0 -- vault login $VAULT_ROOT_TOKEN
```

Start an interactive shell session on the vault-0 pod.
```bash
kubectl exec --stdin=true --tty=true vault-0 -- /bin/sh
```
We are now working from the vault-0 pod.  You should see a prompt, something like:

```bash
/ $
```

Enable the PKI secrets engine at its default path.
```bash
vault secrets enable pki
```

Configure the max lease time-to-live (TTL) to 8760h.
```bash
vault secrets tune -max-lease-ttl=8760h pki
```

# Vault CA key pair

Vault can accept an existing key pair, or it can generate its own self-signed root. In general, they recommend maintaining your root CA outside of Vault and providing Vault a signed intermediate CA, but for this demo we will keep it simple and generate a self signed root certificate.

Generate a self-signed certificate valid for 8760h.
```bash
vault write pki/root/generate/internal common_name=example.com ttl=8760h
```

Configure the PKI secrets engine certificate issuing and certificate revocation list (CRL) endpoints to use the Vault service in the default namespace.
```bash
vault write pki/config/urls issuing_certificates="http://vault.default:8200/v1/pki/ca" crl_distribution_points="http://vault.default:8200/v1/pki/crl"
```

For our local MTLS certificates we will use our kubernetes namespace as our domain name. This will allow us to recongize where these components reside.
For our public TLS certificates for this demo we will use myhpcc.com as our domain.

Configure a role named hpccnamespace that enables the creation of certificates hpccnamespace domain with any subdomains.

```bash
vault write pki/roles/hpcclocal key_type=any allowed_domains=default allow_subdomains=true allowed_uri_sans="spiffe://*" max_ttl=72h
```

Configure a role named myhpcc-dot-com that enables the creation of certificates myhpcc.com domain with any subdomains.

```bash
vault write pki/roles/myhpcc-dot-com allowed_domains=myhpcc.com allow_subdomains=true allowed_uri_sans="spiffe://*" max_ttl=72h
```

Create a policy named pki that enables read access to the PKI secrets engine paths.

```bash
vault policy write pki - <<EOF
path "pki*"                        { capabilities = ["read", "list"] }
path "pki/roles/myhpcc-dot-com"   { capabilities = ["create", "update"] }
path "pki/sign/myhpcc-dot-com"    { capabilities = ["create", "update"] }
path "pki/issue/myhpcc-dot-com"   { capabilities = ["create"] }
path "pki/roles/hpcclocal"   { capabilities = ["create", "update"] }
path "pki/sign/hpcclocal"    { capabilities = ["create", "update"] }
path "pki/issue/hpcclocal"   { capabilities = ["create"] }
EOF
```

Configure Kubernetes authentication
Vault provides a Kubernetes authentication method that enables clients to authenticate with a Kubernetes Service Account Token.

Enable the Kubernetes authentication method.

```bash
vault auth enable kubernetes
```

Configure the Kubernetes authentication method to use the service account token, the location of the Kubernetes host, and its certificate.

```bash
vault write auth/kubernetes/config \
    token_reviewer_jwt="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
    kubernetes_host="https://$KUBERNETES_PORT_443_TCP_ADDR:443" \
    kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
```

Finally, create a Kubernetes authentication role named issuer that binds the pki policy with a Kubernetes service account named issuer.

```bash
vault write auth/kubernetes/role/issuer \
    bound_service_account_names=issuer \
    bound_service_account_namespaces=cert-manager,default \
    policies=pki \
    ttl=20m
```
Exit from the vault pod:

```bash
exit
```

Deploy Cert Manager

Configure an issuer and generate a certificate
The cert-manager enables you to define Issuers that interface with the Vault certificate generating endpoints. These Issuers are invoked when a Certificate is created.

Create a namespace named cert-manager to host the cert-manager.

```bash
kubectl create namespace cert-manager
```

## Install cert-manager custom resource defintions:

This adds new custom resource types to kubernetes for certificate issuers and certificates.

```bash
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.1.0/cert-manager.crds.yaml
```

## Install cert-manager helm chart:

Add Jetstack helm repo:

```bash
helm repo add jetstack https://charts.jetstack.io
```

Install cert-manager.

```bash
helm install cert-manager jetstack/cert-manager --namespace cert-manager --version v1.1.0
```

Create a service account named issuer within the default namespace.

```bash
kubectl create serviceaccount issuer
```

The service account generated a secret that is required by the Issuer.

Get all the secrets in the default namespace.

```bash
kubectl get secrets
```

Create a variable named ISSUER_SECRET_REF to capture the secret name.

```bash
ISSUER_SECRET_REF=$(kubectl get serviceaccount issuer -o json | jq -r ".secrets[].name")
```

## Installing the HPCC with certificate generation enabled

Install the HPCC helm chart with the "--set certificates.enabled" option set to true.

```bash
helm install myhpcc hpcc/ --set global.image.version=latest --set certificates.enabled=true --set certificates.issuers.local.spec.vault.auth.kubernetes.secretRef.name=$ISSUER_SECRET_REF  --set certificates.issuers.public.spec.vault.auth.kubernetes.secretRef.name=$ISSUER_SECRET_REF --values examples/certmanager/values-vault-pki.yaml
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods
```

Check and see if the cerficate issuers have been successfully created.

```bash
kubectl get issuers -o wide
```

You should see something like this:

```bash
NAME                 READY   STATUS           AGE
hpcc-local-issuer    True    Vault verified   78s
hpcc-public-issuer   True    Vault verified   78s
```

Check and see if the cerficates have been successfully created.

```bash
kubectl get certificates
```

You should see something like this:

```bash
NAME                                      READY   SECRET                                   AGE
compile-local-myeclccserver-cert          True    compile-local-myeclccserver-tls          85s
dali-local-mydali-cert                    True    dali-local-mydali-tls                    85s
eclagent-local-hthor-cert                 True    eclagent-local-hthor-tls                 85s
eclagent-local-roxie-workunit-cert        True    eclagent-local-roxie-workunit-tls        85s
eclagent-local-thor-cert                  True    eclagent-local-thor-tls                  85s
eclagent-local-thor-eclagent-cert         True    eclagent-local-thor-eclagent-tls         85s
eclccserver-local-myeclccserver-cert      True    eclccserver-local-myeclccserver-tls      85s
eclqueries-public-eclqueries-cert         True    eclqueries-public-eclqueries-tls         85s
eclservices-local-eclservices-cert        True    eclservices-local-eclservices-tls        85s
eclwatch-public-eclwatch-cert             True    eclwatch-public-eclwatch-tls             85s
esdl-sandbox-public-esdl-sandbox-cert     True    esdl-sandbox-public-esdl-sandbox-tls     85s
hthor-local-hthor-cert                    True    hthor-local-hthor-tls                    85s
roxie-agent-public-roxie-agent-1-cert     True    roxie-agent-public-roxie-agent-1-tls     85s
roxie-agent-public-roxie-agent-2-cert     True    roxie-agent-public-roxie-agent-2-tls     85s
roxie-agent-local-roxie-agent-1-cert      True    roxie-agent-local-roxie-agent-1-tls      85s
roxie-agent-local-roxie-agent-2-cert      True    roxie-agent-local-roxie-agent-2-tls      85s
roxie-local-roxie-workunit-cert           True    roxie-local-roxie-workunit-tls           85s
sql2ecl-public-sql2ecl-cert               True    sql2ecl-public-sql2ecl-tls               85s
dfs-public-dfs-cert                       True    dfs-public-dfs-tls                       85s
thoragent-local-thor-thoragent-cert       True    thoragent-local-thor-thoragent-tls       85s
thormanager-local-thormanager-w-cert      True    thormanager-local-thormanager-w-tls      85s
thorworker-local-thorworker-w-cert        True    thorworker-local-thorworker-w-tls        85s
topo-local-roxie-toposerver-cert          True    topo-local-roxie-toposerver-tls          85s
udpkey-udp-roxie-cert                     True    udpkey-udp-roxie-dtls                    85s
```

List the kubernetes secrets, which now include the generated tls secrets.

```bash
kubectl get secrets
```

You should see something like this:

```bash
NAME                                     TYPE                                  DATA   AGE
cert-manager-cainjector-token-wmdxq      kubernetes.io/service-account-token   3      3m52s
cert-manager-token-jtlgx                 kubernetes.io/service-account-token   3      3m52s
cert-manager-webhook-ca                  Opaque                                3      3m51s
cert-manager-webhook-token-df4xk         kubernetes.io/service-account-token   3      3m52s
compile-local-myeclccserver-tls          kubernetes.io/tls                     3      2m49s
dali-local-mydali-tls                    kubernetes.io/tls                     3      2m56s
default-token-kmj97                      kubernetes.io/service-account-token   3      2d1h
eclagent-local-hthor-tls                 kubernetes.io/tls                     3      2m55s
eclagent-local-roxie-workunit-tls        kubernetes.io/tls                     3      2m53s
eclagent-local-thor-eclagent-tls         kubernetes.io/tls                     3      2m56s
eclagent-local-thor-tls                  kubernetes.io/tls                     3      2m54s
eclccserver-local-myeclccserver-tls      kubernetes.io/tls                     3      2m55s
eclqueries-public-eclqueries-tls         kubernetes.io/tls                     3      2m52s
eclservices-local-eclservices-tls        kubernetes.io/tls                     3      2m54s
eclwatch-public-eclwatch-tls             kubernetes.io/tls                     3      2m50s
esdl-sandbox-public-esdl-sandbox-tls     kubernetes.io/tls                     3      2m49s
hpcc-agent-token-h78cd                   kubernetes.io/service-account-token   3      2m58s
hpcc-default-token-55lss                 kubernetes.io/service-account-token   3      2m58s
hpcc-local-issuer-key-pair               kubernetes.io/tls                     2      3m23s
hpcc-thoragent-token-xkm7j               kubernetes.io/service-account-token   3      2m58s
hthor-local-hthor-tls                    kubernetes.io/tls                     3      2m49s
myhpcc-filebeat-token-vjplq              kubernetes.io/service-account-token   3      2m58s
roxie-agent-public-roxie-agent-1-tls     kubernetes.io/tls                     3      2m51s
roxie-agent-public-roxie-agent-2-tls     kubernetes.io/tls                     3      2m49s
roxie-agent-local-roxie-agent-1-tls      kubernetes.io/tls                     3      2m51s
roxie-agent-local-roxie-agent-2-tls      kubernetes.io/tls                     3      2m52s
roxie-local-roxie-workunit-tls           kubernetes.io/tls                     3      2m52s
sh.helm.release.v1.cert-manager.v1       helm.sh/release.v1                    1      3m52s
sh.helm.release.v1.myhpcc.v1             helm.sh/release.v1                    1      2m58s
sql2ecl-public-sql2ecl-tls               kubernetes.io/tls                     3      2m55s
dfs-public-dfs-tls                       kubernetes.io/tls                     3      2m55s
thoragent-local-thor-thoragent-tls       kubernetes.io/tls                     3      2m52s
thormanager-local-thormanager-w-tls      kubernetes.io/tls                     3      2m51s
thorworker-local-thorworker-w-tls        kubernetes.io/tls                     3      2m51s
topo-local-roxie-toposerver-tls          kubernetes.io/tls                     3      2m53s
udpkey-udp-roxie-dtls                    kubernetes.io/tls                     3      2m55s
```

The cluster ESPs are now using TLS both locally and publicly.

Run an ecl job that requires using mutual TLS (using local client certificate):

```
ecl run --ssl hthor examples/certmanager/localhttpcall.ecl
```

Note that for the HTTPCALL in our ecl example the url now starts with "mtls:" this tells HTTPCALL/SOAPCALL to use mutual TLS, using the local client certificate, and to verify the server using the local certificate authority certificate.

You should see a result similar to this:

```xml
<Result>
<Dataset name='localHttpEchoResult'>
 <Row><Method>GET</Method><UrlPath>/WsSmc/HttpEcho</UrlPath><UrlParameters>name=doe,joe&amp;number=1</UrlParameters><Headers><Header>Accept-Encoding: gzip, deflate</Header><Header>Accept: text/xml</Header></Headers><Content></Content></Row>
</Dataset>
</Result>
```
