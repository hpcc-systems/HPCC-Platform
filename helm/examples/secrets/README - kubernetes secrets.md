# Containerized HPCC Systems Secrets

This example demonstrates HPCC use use of Kubernetes secrets.

This example assumes you are starting from a linux command shell in the HPCC-Platform/helm directory.  From there you will find the example files and this README file in the examples/secrets directory.

## 'eclUser' category secrets

Create example 'eclUser' secret:

```bash
kubectl create secret generic k8s-example --from-file=crypt.key=examples/secrets/crypt.key
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

Create example kubernetes secret:

```bash
kubectl create secret generic http-connect-basicsecret --from-file=url=examples/secrets/url-basic --from-file=examples/secrets/username --from-file=examples/secrets/password
```

## Installing the HPCC with the secrets added to ECL components

Install the HPCC helm chart with the secrets just defined added to all components that run ECL.

```bash
helm install myhpcc hpcc/ --set global.image.version=latest -f examples/secrets/values-secrets.yaml
```

Use kubectl to check the status of the deployed pods.  Wait until all pods are running before continuing.

```bash
kubectl get pods
```
--------------------------------------------------------------------------------------------------------

If you don't already have the HPCC client tools installed please install them now:

https://hpccsystems.com/download#HPCC-Platform


## Using the created 'eclUser' category secrets directly in ECL code

The following ecl commands will run the three example ECL files on hthor.

```bash
ecl run hthor examples/secrets/crypto_kubernetes_secret.ecl
```

The expected result would be:

```xml
<Result>
<Dataset name='k8s_message'>
 <Row><k8s_message>top secret</k8s_message></Row>
</Dataset>
</Result>
```

## Using the created 'ecl' category secrets via HTTPCALL from within ECL code

If you don't already have the HPCC client tools installed please install them now:

https://hpccsystems.com/download#HPCC-Platform

--------------------------------------------------------------------------------------------------------

The following ecl commands will run the three example ECL files on hthor.

```bash
ecl run hthor examples/secrets/httpcall_secret.ecl

For each job the expected result would be:

```xml
<Result>
<Dataset name='Result 1'>
 <Row><authenticated>true</authenticated></Row>
</Dataset>
</Result>
```
