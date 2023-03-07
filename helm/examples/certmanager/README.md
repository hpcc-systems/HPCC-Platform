# HPCC Systems Certificates using JetStack cert-manager

This example demonstrates HPCC TLS configuration using Jetstack cert-manager.

## Jetstack cert-manager support:

The following will use cert-manager to automatically provision and manage TLS certificates for the
HPCC.

The following steps can be used to set up cert-manager in a kubernetes cluster.

--------------------------------------------------------------------------------------------------------

## Install cert-manager helm chart:

Add Jetstack helm repo:

```bash
helm repo add jetstack https://charts.jetstack.io
```

Install cert-manager.

```bash
helm install cert-manager jetstack/cert-manager --set installCRDs=true --namespace cert-manager --create-namespace
```

## Run from HPCC-Platform/helm directory

For now this example will assume you are in the helm directory of the HPCC-Systems source.

## Create a root certificate for our local cluster certificate authority

This example uses OpenSSL to generate the root certificate for our local cluster certificate authority.

We can create a root certificate and private key for our local cluster certificate authority with
a single openssl call. This call uses the openssl config file found in the examples directory (ca-req.cfg).


```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -sha256 -days 1825 -out ca.crt -config examples/certmanager/ca-req.cfg
```

For additonal information on the openssl command being used checkout this link:
https://www.openssl.org/docs/man1.0.2/man1/openssl-req.html

For a general overview check out this link:
https://www.golinuxcloud.com/create-certificate-authority-root-ca-linux


## Create a Kubernetes TLS secret from the generated root certificate and privatekey

The root certificate needs to be added as a kubernetes secret in order to be accessible to cert-manager.
The secret name matches the default name used in the local issuer configuration in values.yaml.

```bash
kubectl create secret tls hpcc-local-issuer-key-pair --cert=ca.crt --key=ca.key
```


## ECL code signing root certificate

Repeat the root certificate creation process to create the CA secret for the code signing issuer.

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout signingca.key -sha256 -days 1825 -out signingca.crt -config examples/certmanager/ca-req.cfg
```

## Create a Kubernetes TLS secret from the generated signing root certificate and privatekey

```bash
kubectl create secret tls hpcc-signing-issuer-key-pair --cert=signingca.crt --key=signingca.key
```


## Installing the HPCC with certificate generation enabled

Install the HPCC helm chart with the "--set certificates.enabled" option set to true.

```bash
helm install myhpcc hpcc/hpcc --set certificates.enabled=true
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
NAME                   READY   STATUS                AGE
hpcc-public-issuer     True                          3m57s
hpcc-local-issuer      True    Signing CA verified   3m57s
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

The default public issuer uses self signed certificates. This makes it very easy to set up but browsers
will not recognize the certificates as trustworthy and the browser will warn users that the connection
is not safe.

How to set up a cert-manager ClusterIssuer to use something like LetsEncrypt or Zerossl is outside the
scope of this document, but once you have one set up you can use it to generate your public certificates
by setting the public issuer values something like this:

certificates:
  issuers:
    public:
      name: zerossl-issuer
      kind: ClusterIssuer

Where "zerossl-issuer" is the name of the externally defined cert-manager ClusterIssuer you wish to use.
