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
import signal
import sys
import time
import datetime
from kubernetes import client, config, watch

def signal_handler(signum, frame):
    sys.exit(0)

def current_time():
  ts = time.time()
  return datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H:%M:%S')

def main():
    # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.
    #config.load_kube_config()
    config.load_incluster_config()

    v1 = client.CoreV1Api()
    current_namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()

    event_log = "/tmp/pod_events.log"
    if os.path.exists(event_log):
       os.remove(event_log)

    event_err = "/tmp/pod_events_error.log"
    if os.path.exists(event_err):
       os.remove(event_err)

    config_log = "/tmp/hpcc_config.log"
    if os.path.exists(config_log):
       os.remove(config_log)

    not_ready_list = []
    resource_version=0;
    while True:

       #print("Wait for events ...")
       #f_event  = open (event_log, 'a')
       #f_event.write("%s - Wait for events ... \n" % (current_time()))
       #f_event.write("Number of not ready pods: %d\n" % (len(not_ready_list)))
       #f_event.close()
       w = watch.Watch()
       try:
          new_running_list = []
          f_event  = open (event_log, 'a')
          for event in w.stream(v1.list_namespaced_pod, namespace=current_namespace, timeout_seconds=5, resource_version=resource_version):
             #print("Event: %s %s %s %s" % (event['type'],event['object'].kind, event['object'].metadata.name, event['object'].status.phase))
             f_event.write("%s - Event: %s %s %s %s\n" % (current_time(),event['type'],event['object'].kind, event['object'].metadata.name, event['object'].status.phase))
             #print("Resource_version: %s" % (event['object'].metadata.resource_version))
             if int(resource_version) < int(event['object'].metadata.resource_version):
                resource_version = event['object'].metadata.resource_version
             pod_name = event['object'].metadata.name
             state = event['object'].status.phase
             if (pod_name.startswith('dali')      or
                 pod_name.startswith('esp')       or
                 pod_name.startswith('thor')      or
                 pod_name.startswith('thor_roxie') or
                 pod_name.startswith('roxie')     or
                 pod_name.startswith('eclcc')     or
                 pod_name.startswith('scheduler') or
                 pod_name.startswith('backup')    or
                 pod_name.startswith('sasha')     or
                 pod_name.startswith('dropzone')  or
                 pod_name.startswith('support')   or
                 pod_name.startswith('spark')     or
                 pod_name.startswith('ldap')     or
                 pod_name.startswith('node')):

                 if state == 'Running':
                    if pod_name not in new_running_list:
                       new_running_list.append(pod_name)
                    if pod_name in not_ready_list:
                       not_ready_list.remove(pod_name)
                 else:
                    if pod_name not in not_ready_list:
                       not_ready_list.append(pod_name)

          f_event.close()
          #print("not_ready_list size: %d, new_running_list size: %d" % (len(not_ready_list), len(new_running_list)))
          if len(not_ready_list) == 0 and len(new_running_list) > 0:
             # Configure and restart HPCC cluster
             #print("/opt/hpcc-tools/config_hpcc.sh")
             cmd = "/opt/hpcc-tools/config_hpcc.sh > " + config_log + " 2>&1"
             os.system(cmd)

       except Exception as e:
             #print(e)
             f_error  = open (event_err, 'a')
             f_error.write(str(e))
             f_error.close()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
