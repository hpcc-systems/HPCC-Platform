# Placements

The placement is responsible for finding the best Node for a Pod. It can be configured
through placement to include an array of objects to configure the Kubernetes Scheduler.
It must have a "pods" list which tells it which pod the settings will be applied to.<br/>
The syntax is:
```code
   placements:
   - pods: [list]
     placement:
       <supported configurations>
```
The list item in "pods" can be one of the following:
1) HPCC Systems component types in format: "type:<type name>". <type name> includes
   dali, esp, eclagent, eclccserver, roxie, thor. For example "type:esp"
2) Target, the name of array item of above type in format "target:<target name>"
   For example "target:roxie", "target:thor".
3) Pod, "Deployment" metadata name, which usually should be from the name of the array
   item of a type. For example, "eclwatch", "mydali", "thor-thoragent"..
4) Job name regular expression:  For example "compile-" or "compile-.*" or exact match "^compile-.*$"
5) set pod: ["all"] for all HPCC Systems components

Regardless of the order of placements in the configuration, they will be processed in the following order: "all", "type", "target" and "pod" or "job".

Supported configurations under each "placement"
1) nodeSelector
   Multiple nodeSelectors can be applied. For example
   placements:
   - pods: ["all"]
     placement:
       nodeSelector:
         group: "hpcc"
   - pods: ["type:dali"]
     placement:
       nodeSelector:
         spot: "false"
   All dali pods will have:
     spec:
       nodeSelector:
         group: "hpcc"
         spot: "false"
   If duplicated keys are defined only the last one will prevail.

2) taints/tolerations
   Multiple taints/tolerations can be applied. For example
   placements:
   - pods: ["all"]
     tolerations:
     - key: "group"
       operator: "Equal"
       value: "hpcc"
       effect: "NoSchedule"
   - pods: ["type:thor"]
     tolerations:
     - key: "gpu"
       operator: "Equal"
       value: "true"
       effect: "NoSchedule"
   All thor pods will have:
     tolerations:
     - key: "group"
       operator: "Equal"
       value: "hpcc"
       effect: "NoSchedule"
     - key: "gpu"
       operator: "Equal"
       value: "true"
       effect: "NoSchedule"

3) affinity
   There is no schema check for the content of affinity. Reference
   https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
   Only one "affinity" can be applied to a Pod/Job. If a Pod/Job matches multiple placement 'pods' lists, then only the last "affinity" definition will apply.

4) schedulerName: profile names. "affinity" defined in scheduler profile  requires
   Kubernetes 1.20.0 beta and later releases
   Only one "schedulerName" can be applied to a Pod/Job.

5) topologySpreadConstraints
   Requires Kubernetes v1.19+.
   Reference https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/

"nodeSelector" example:
```code
placements:
- pods: ["eclwatch","roxie-workunit","^compile-.*$","mydali"]
  placement:
    nodeSelector:
      name: npone
```
"nodeSelector" and "tolerations" example:
```code
- pods: ["thorworker-", "thor-thoragent", "thormanager-","thor-eclagent","hthor-"]
  placement:
    nodeSelector:
      name: nptwo
    tolerations:
    - key: "name"
      operator: "Equal"
      value: "nptwo"
      effect: "NoSchedule"
```
"affinity" example:
```code
- pods: ["thorworker-.*"]
  placement:
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: kubernetes.io/e2e-az-name
              operator: In
              values:
              - e2e-az1
              - e2e-az2
```
"schedulerName" example, above "affinity" settings can be in this file. Caution "affinity" in "schedulerName" file is only supported in Kubernetes 1.20.0 beta and later.
```code
- pods: ["target:roxie"]
  placement:
    schedulerName: "my-scheduler"
#The settings will be applied to all thor pods/jobs and myeclccserver pod and job
- pods: ["target:myeclccserver", "type:thor"]
  placement:
    nodeSelector:
      app: "tf-gpu"
    tolerations:
    - key: "gpu"
      operator: "Equal"
      value: "true"
      effect: "NoSchedule"

```
"topologySpreadConstraints" example, there are two node pools which have "hpcc=nodepool1" and "hpcc=nodepool2" respectively. The Roxie pods will be evenly scheduled on the two node pools. After deployment verify it with
```code
kubectl get pod -o wide | grep roxie
```
Placements code:
```code
- pods: ["type:roxie"]
  placement:
    topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: hpcc
      whenUnsatisfiable: ScheduleAnyway
      labelSelector:
        matchLabels:
          roxie-cluster: "roxie"
```
