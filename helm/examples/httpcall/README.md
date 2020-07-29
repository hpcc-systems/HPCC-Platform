# Containerized HPCC Systems HTTPCALL and SOAPCALL Secrets

## HPCC Systems HTTPCALL Secrets

## About HTTP-CONNECT Secrets:

HTTP-CONNECT secrets consist of a url string and optional additional secrets associated with that URL.  If we supported providing credentials
without an associated url then those credentials could be sent anywhere and wouldn't be secret very long.

Besides the URL values can currently be set for proxy (tusted for keeping these secrets), username, password, tls.key
(client certificate key), tls.crt (client certificate).

## Creating the HTTP-CONNECT Secrets

Example secret - basic auth URL:

kubectl create secret generic http-connect-urlsecret --from-file=url=url-combined


Example secret - basic auth with credentials:

kubectl create secret generic http-connect-basicsecret --from-file=url=url-basic --from-file=username --from-file=password


Example client certificate secret: (WIP)

kubectl create secret generic http-connect-certsecret --from-file=url=url-cert  --from-file=tls.key --from-file=tls.crt


## HELM Installing the HPCC with the HTTP-CONNECT Secrets added to ECL components

Install the HPCC helm chart with the secrets just defined added to all components that run ECL.

helm install httpconnect hpcc/ --set global.image.version=latest -f examples/httpcall/values-http-connect.yaml


## ECL test

ecl run hthor httpcall.ecl
