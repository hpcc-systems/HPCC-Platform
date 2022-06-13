{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "hpcc-azurefile.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hpcc-azurefile.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hpcc-azurefile.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "hpcc-azurefile.labels" -}}
helm.sh/chart: {{ include "hpcc-azurefile.chart" . }}
{{ include "hpcc-azurefile.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "hpcc-azurefile.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hpcc-azurefile.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account
*/}}
{{- define "hpcc-azurefile.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "hpcc-azurefile.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Add common mountOptions
*/}}
{{- define "hpcc-azurefile.addCommonMountOptions" }}
mountOptions:
- uid=10000 # uid of user 'hpcc'
- gid=10001 # gid of group 'hpcc'
- dir_mode=0700 # user read/write/execute
- file_mode=0600 # user read/write
{{- if .plane.shareName }}
- mfsymlinks
- cache=strict
- nosharesock
- nobrl
{{- end -}}
{{- end }}

{{/*
Create PersistentVolume
Pass in dict with root and plane
*/}}
{{- define "hpcc-azurefile.addStoragePV" -}}
{{- $common := .root.Values.common -}}
{{- $shareName := .plane.shareName -}}
apiVersion: v1
kind: PersistentVolume
metadata:
  name: {{ include "hpcc-azurefile.PVName" (dict "root" .root "plane" .plane) }}
  labels:
    store: {{ include "hpcc-azurefile.PVName" (dict "root" .root "plane" .plane) }}
spec:
  capacity:
    storage: {{ .plane.size }}
  accessModes:
    - {{ .plane.rwmany | default false | ternary "ReadWriteMany" "ReadWriteOnce" }}
  persistentVolumeReclaimPolicy: Retain
  storageClassName: azurefile-csi
  csi:
    driver: file.csi.azure.com
    readOnly: false
    volumeHandle: {{ .plane.volumeId | default (printf "100-%s" $shareName) }}  # make sure this volumeid is unique in the cluster
    volumeAttributes:
      resourceGroup: EXISTING_RESOURCE_GROUP_NAME  # optional, only set this when storage account is not in the same resource group as agent node
      shareName: {{ .plane.shareName }}
    nodeStageSecretRef:
      name: {{ .plane.secretName | default $common.secretName }}
      namespace: {{ .plane.secretNamespace | default $common.secretNamespace }}
  {{- include "hpcc-azurefile.addCommonMountOptions" . | indent 2}}
{{- end -}}

{{/*
Create PersistentVolume name
Pass in dict with root and plane
*/}}
{{- define "hpcc-azurefile.PVName" -}}
{{ printf "%s-%s-pv" .plane.name (include "hpcc-azurefile.fullname" .root) | trunc 63 }}
{{- end -}}

{{/*
Create StorageClass name
Pass in dict with root and plane
*/}}
{{- define "hpcc-azurefile.SCName" -}}
{{- if not .plane.shareName -}}
{{ printf "%s-%s-sc" .plane.name (include "hpcc-azurefile.fullname" .root) | trunc 63 }}
{{- end -}}
{{- end -}}

{{/*
Create StorageClass
Pass in dict with root and plane
*/}}
{{- define "hpcc-azurefile.addStorageSC" -}}
{{- $common := .root.Values.common -}}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ include "hpcc-azurefile.SCName" (dict "root" .root "plane" .plane) }}
provisioner: {{ $common.provisioner }}
{{- include  "hpcc-azurefile.addCommonMountOptions" . }}
parameters:
  skuName: {{ .plane.sku | default "Standard_LRS" }}
reclaimPolicy: Retain
allowVolumeExpansion: true
{{- end -}}

{{/*
Add PV selector
Pass in dict with root and plane
*/}}
{{- define "hpcc-azurefile.addPVSelector" -}}
{{- if .plane.shareName -}}
selector:
  matchLabels:
    store: {{ include "hpcc-azurefile.PVName" (dict "root" .root "plane" .plane) }}
{{- end -}}
{{- end -}}
