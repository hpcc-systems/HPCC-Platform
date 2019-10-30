### Collect the Cluster IPs
- CollectIPs.py: base class to collect cluster ips

### Generate environment.xml
- config_hpcc.sh: entry point to configure HPCC System cluster:
  ..1. Collect cluster ips
  ..2. Generate environment.xml
  ..3. Configure Ansible hosts
  ..4. Stop HPCC Systems cluster
  ..5. Push the environment.xml to HPCC Systems cluster
  ..6. Start HPCC Systems cluster
  ..7. Print out HPCC Systems cluster layout

### Ansible
- ansible: setup file and playbook yaml files
- Help script:
  ..* push_env.sh
  ..* start_hpcc.sh
  ..*  stop_hpcc.sh

### Providers
each provider directory includes:
.1 IP collector scripts
.2 optional cluster monitoring script
.3 environment.xml generation scripts

- docker
- kube

To do:
- AWS ECS
- Azure docker deployment without Kubernetes
- GCE docker deployment without Kubernetes

### Admin entrypoint script
- run: admin container entrypoing script
