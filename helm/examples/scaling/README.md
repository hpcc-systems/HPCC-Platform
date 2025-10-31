# Kubernetes Scaling

A major feature provided by Kubernetes environments is the ability to automatically scale workload resources (such as a Deployment or StatefulSet), to match actual workloads. Scaling workload resources or available nodes dynamically is a powerful way to manage performance and associated costs.

Kubernetes provides 3 main types of scaling: Horizontal (HPA), Vertical (VPA), and Cluster. HPA adjusts the replica count of a given application, whereas VPA adjusts the resource requests and limits of a container. The number of nodes available to a cluster is also adjustable.

## Horizontal Pod Autoscaler (HPA)

HPAs automatically adjusts a workload resource to handle actual demand by increasing or decreasing the number of pods as opposed to adjusting resources available to running pods. When the actual load recedes, and current pods count is above the declared minimum, the HPA directs the affected workload resource to scale back down.

HPA Kubernetes objects are directly associated with the workload resource to be automatically scaled, along with the desired min and max pod count, and a metric threshold used to determine when to scale up or down. There are 3 major metric types tracked by HPAs, resource (CPU, Memory), custom (specific to an appliation within the container), and external (stemming from outside the cluster). 

For HPAs to function, a source of metrics must be available. Basic resource metrics (CPU, Memory) are collected and made available by The Kubernetes Metrics Server. This server must be configured and deployed for resource metrics based HPAs to function. Metric Server deployment notes can be foud [here](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/)

See this document for [K8s HPA details](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/).

### HPCC Systems HPA support

Some of HPCC Systems' components are candidates for HPA configuration (ESP, ECLCCServer, etc.) and can be configured directly within the HPCC helm deployment.

### HPCC Systems HPA Examples

HPCC Systems component HPAs are centrally declared in global.hpas and referenced from within the component to be scaled.

#### Example centralized HPAs configuration

```yaml
+global:
+  hpas: #list of named HPA configurations, to be referenced from component definitions
+    HighCPULowReplicaHPAType: #example HPA profile for high CPU scaling trigger, with low replica count
+      hpa:
+        minReplicas: 1
+        maxReplicas: 2
+        metrics:
+        - type: Resource
+          name: cpu
+          target:
+            type: Utilization
+            value: "90"
+    LowCPULowReplicaHPAType: #example HPA profile for low CPU scaling trigger, with low replica count
+      hpa:
+        minReplicas: 1
+        maxReplicas: 2
+        metrics:
+        - type: Resource
+          name: cpu
+          target:
+            type: Utilization
+            value: "5"
```
#### Example 1: ESP ECLWatch Auto-scaling

Example ESP application 'eclwatch' configured to horizontally auto-scale from 1 up to 2 replicas based on cpu utilization of 90 by referencing 'HighCPULowReplicaHPAType':

```yaml
esp:
- name: eclwatch
  application: eclwatch
  auth: none
  replicas: 1
  hpa: "HighCPULowReplicaHPAType" # Reference to a named HPA configuration defined in global HPAs
```

#### Example 2: DaFileSrv Rowservice Elastic Scaling

The DaFileSrv rowservice component provides streaming file access capabilities and is designed for elastic scaling based on actual demand. 

**On-Demand Scaling Characteristics:**
- Scales up automatically when data stream requests increase CPU utilization
- Can scale down to minimal replicas when streaming demand is low
- Responds to burst traffic patterns typical of file access workloads
- Maintains secure mTLS communication during scaling operations

```yaml
dafilesrv:
  - name: rowservice
    disabled: false
    application: stream
    service:
      servicePort: 7600
      visibility: global
    resources:
      cpu: "500m"
      memory: "128Mi"
    hpa: "LowCPULowReplicaHPAType" # Reference to a named HPA configuration defined in global HPAs
```

**Prerequisites for DaFileSrv HPA:**
- Kubernetes Metrics Server deployment
- Resource requests configured for all containers (including postrun sidecars)

Monitor CPU and memory utilization patterns to optimize resource allocation for your specific file access patterns and throughput requirements.

Example rowservice deployment with HPA for automated scaling based on CPU using provided sample files:

```console
# Deploy HPCC Platform with rowservice HPA enabled
helm install myhpcc hpcc/hpcc --set global.image.version=latest \
  -f helm/examples/scaling/sample-hpas.yaml -f helm/examples/scaling/rowservice-LowCPULowReplicaHPA.yaml

# Monitor autoscaling behavior during streaming workloads
kubectl get hpa rowservice-hpa -w

# Observe scaling response to demand
kubectl get deployment rowservice
```

Otherwise, an HPA can be created manually utilizing the kubectl command line tool.

Example HPA to autoscale the eclwatch deployment based on CPU utilization at 80%, with a count of pods from 1 to a maxium of 3:
```console
  kubectl autoscale deployment eclwatch --cpu-percent=80 --min=1 --max=3
```

HPA behavior can be further configured by the hpa.behavior section for both up and down scaling. This section is optional and default values are assigned. Exact definitions are provided in the "behavior" section of the Kubernetes HPA syntax doc linked below.

HPCC Systems supports the Kubernetes HPA version 2 sytax as defined [here](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/horizontal-pod-autoscaler-v2/#HorizontalPodAutoscalerSpec)

## Vertical Pod Autoscaler (VPA)

Vertical Pod Autoscaler (VPA) is a Kubernetes feature that helps determine the appropriate ammount of resource requests associated with application pods. Kubernetes scheduler does not re-evaluate the pod’s resource requirements once they have been deployed with a given set of requests. If the pod is initially over-allocated resources, those resources can go unused and the cost will be unnecessarily affected. On the other hand, if a pod is not afforded sufficient resources, performance will suffer and pods might be removed. VPA helps allocate the appropriate ammount of resources.

When enabled and configured VPA sets the requests automatically based on usage and allow proper scheduling onto nodes allowing suffient resources be made available for each pod. It will also maintain ratios between limits and requests that were specified in initial containers configuration.

Similarly to HPA, the Kubernetes Metrics Server must be deployed on the cluster in order to make the VPAs funtional. Read more about [Metrics Server](https://github.com/kubernetes-sigs/metrics-server).

The k8s VPA needs to be installed as well. Read more about [K8s VPA Installing](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler#installation)

VPAs must be manually created, as HPCC does not currently support integrated VPA configuration.

## Cluster Autoscaler
Cluster Autoscaler (CA) automatically increases or decreases the number of cluster nodes due to insufficient resources to run a pod (adds a node) or when a node remains underutilized, and its pods can be assigned to another node (removes a node).
