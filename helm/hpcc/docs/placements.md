# Placements

The placement is responsible for finding the best Node for a Pod. It can be configured
through placement to include an array of objects to configure the Kubernetes Scheduler.
It must have a "pods" list which tells it which pod the settings will be applied to.<br/>
The syntax is:
```code
   global
     placements:
     - pods [list]
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

Supported configurations under each "placement"
1) nodeSelector
2) taints/tolerations
3) affinity
   There is no schema check for the content of affinity. Reference
   https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
4) schedulerName: profile names. "affinity" defined in scheduler profile  requires
   Kubernetes 1.20.0 beta and later releases

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
