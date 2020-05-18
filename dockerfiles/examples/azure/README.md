This directory contains an example of using HPCC containers with persistent files.

Use:
 helm install <pvcname> azhpcc            - create the persistent volume claims to store the data
 helm install hpcc/latest  --values azure.yaml   - install hpcc system using azure specific configuration
