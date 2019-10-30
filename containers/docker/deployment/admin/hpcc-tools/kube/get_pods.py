#!/usr/bin/python3

import os.path
from kubernetes import client, config


def main():
    # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.
    #config.load_kube_config()
    config.load_incluster_config()

    v1 = client.CoreV1Api()
    current_namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
    #print("Listing pods with their IPs namespace " + current_namespace)
    pods = v1.list_namespaced_pod(current_namespace).items
    file_name = "/tmp/pods_ip.lst"
    f_ips  = open (file_name, 'w')
    for pod in pods:
        f_ips.write(pod.metadata.name + " " + pod.status.pod_ip + "\n")


if __name__ == '__main__':
    main()
