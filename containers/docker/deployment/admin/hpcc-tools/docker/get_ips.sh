#!/bin/bash -e

networkName=hpcc_ovnet
[ -n "$1" ] && networkName=$1

echo "wget http://${HOST_IP}:2375/networks/${networkName}"
wget http://${HOST_IP}:2375/networks/${networkName}
mv ${networkName} /tmp/${networkName}.json
