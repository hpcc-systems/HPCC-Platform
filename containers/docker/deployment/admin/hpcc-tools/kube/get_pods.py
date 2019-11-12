#!/usr/bin/python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

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
