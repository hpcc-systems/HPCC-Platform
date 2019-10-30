#!/bin/bash

KUBE_TOKEN=$(</var/run/secrets/kubernetes.io/serviceaccount/token)
wget --no-check-certificate --header="Authorization: Bearer $KUBE_TOKEN"  \
 https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_PORT_443_TCP_PORT/api/v1/pods  -O pods.json
