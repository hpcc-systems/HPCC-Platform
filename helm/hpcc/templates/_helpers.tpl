{{/*
Expand the name of the chart.
*/}}
{{- define "hpcc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hpcc.fullname" -}}
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
{{- define "hpcc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Translate a port list to a comma-separated list
*/}}
{{- define "hpcc.portListToCommas" -}}
 {{- if hasPrefix "[]" (typeOf .) -}}
  {{- $local := dict "first" true -}}
  {{- range $key, $value := . -}}{{- if not $local.first -}},{{- end -}}{{- $value -}}{{- $_ := set $local "first" false -}}{{- end -}}
 {{- else -}}
  {{- . -}} 
 {{- end -}}
{{- end -}}

{{/*
Generate global ConfigMap info
Pass in root as .
*/}}
{{- define "hpcc.generateGlobalConfigMap" -}}
{{- $local := dict "defaultEsp" "" -}}
imageVersion: {{ required "Please specify .global.image.version" .Values.global.image.version | quote }}
singleNode: {{ .Values.global.singleNode | default false }}
defaultEsp: {{ .Values.global.defaultEsp | default ""}}
{{ if hasPrefix "[]" (typeOf .Values.esp) -}}
esp:
{{ toYaml .Values.esp }}
{{ end -}}
secretTimeout: {{ .Values.secrets.timeout | default 300 }}
storage:
  ##The following is a temporary solution to allow blob storage to be tested
  ##This will be completely rewritten and restructured to encompass the idea of multiple storage planes.
  ##The source of the information is likely to be move to .Values.storage rather than .Values.global
  default:
{{- if .Values.global.defaultDataPath }}
    data: {{ .Values.global.defaultDataPath }}
{{- end }}
{{- if .Values.global.defaultMirrorPath }}
    mirror: {{ .Values.global.defaultMirrorPath }}
{{- end }}
{{- end -}}

{{/*
Generate local logging info, merged with global
Pass in dict with root and me
*/}}
{{- define "hpcc.generateLoggingConfig" -}}
{{- $logging := deepCopy .me | mergeOverwrite .root.Values.global }}
{{- if hasKey $logging "logging" }}
logging:
{{ toYaml $logging.logging | indent 2 }}
{{- end -}}
{{- end -}}


{{/*
Add ConfigMap volume mount for a component
*/}}
{{- define "hpcc.addConfigMapVolumeMount" -}}
- name: {{ .name }}-configmap-volume
  mountPath: /etc/config
{{- end -}}

{{/*
Add ConfigMap volume for a component
*/}}
{{- define "hpcc.addConfigMapVolume" -}}
- name: {{ .name }}-configmap-volume
  configMap:
    name: {{ .name }}-configmap
{{- end -}}

{{/*
Add data volume mount
*/}}
{{- define "hpcc.addDataVolumeMount" -}}
- name: datastorage-pv
  mountPath: "/var/lib/HPCCSystems/hpcc-data"
{{- end -}}

{{/*
Add data volume
*/}}
{{- define "hpcc.addDataVolume" -}}
- name: datastorage-pv
  persistentVolumeClaim:
    claimName: {{ .Values.storage.dataStorage.existingClaim | default (printf "%s-datastorage-pvc" (include "hpcc.fullname" .)) }}
{{- end -}}

{{/*
Add dll volume mount
*/}}
{{- define "hpcc.addDllVolumeMount" -}}
- name: dllstorage-pv
  mountPath: "/var/lib/HPCCSystems/queries"
{{- end -}}

{{/*
Add dll volume
*/}}
{{- define "hpcc.addDllVolume" -}}
- name: dllstorage-pv
  persistentVolumeClaim:
    claimName: {{ .Values.storage.dllStorage.existingClaim | default (printf "%s-dllstorage-pvc" (include "hpcc.fullname" .)) }}
{{- end -}}

{{/*
Add the secret volume mounts for a component
*/}}
{{- define "hpcc.addSecretVolumeMounts" -}}
{{- $component := .component -}}
{{- $categories := .categories -}}
{{- range $category, $key := .root.Values.secrets -}}
 {{- if (has $category $categories) -}}
{{- range $secretid, $secretname := $key -}}
- name: secret-{{ $secretid }}
  mountPath: /opt/HPCCSystems/secrets/{{ $secretid }}
{{ end -}}
 {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add Secret volume for a component
*/}}
{{- define "hpcc.addSecretVolumes" -}}
{{- $component := .component -}}
{{- $categories := .categories -}}
{{- range $category, $key := .root.Values.secrets -}}
 {{- if (has $category $categories) -}}
{{- range $secretid, $secretname := $key -}}
- name: secret-{{ $secretid }}
  secret:
    secretName: {{ $secretname }}
{{ end -}}
 {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add config arg for a component
*/}}
{{- define "hpcc.configArg" -}}
"--config=/etc/config/{{ .name }}.yaml"
{{- end -}}

{{/*
Add dali arg for a component
*/}}
{{- define "hpcc.daliArg" -}}
"--daliServers={{ (index .Values.dali 0).name }}"
{{- end -}}

{{/*
Get image name
*/}}
{{- define "hpcc.imageName" -}}
{{- /* Pass in a dictionary with root and me defined */ -}}
{{- if .me.image -}}
{{ .me.image.root | default .root.Values.global.image.root | default "hpccsystems" }}/{{ .me.image.name | default .root.Values.global.image.name | default "platform-core" }}:{{ .me.image.version | default .root.Values.global.image.version }}
{{- else -}}
{{ .root.Values.global.image.root | default "hpccsystems" }}/{{ .root.Values.global.image.name | default "platform-core" }}:{{ .root.Values.global.image.version }}
{{- end -}}
{{- end -}}

{{/*
Add image attributes for a component 
Pass in a dictionary with root, me and imagename defined
*/}}
{{- define "hpcc.addImageAttrs" -}}
image: {{ include "hpcc.imageName" . | quote }}
{{ if .me.image -}}
imagePullPolicy: {{ .me.image.pullPolicy | default .root.Values.global.image.pullPolicy | default "IfNotPresent" }}
{{- else -}}
imagePullPolicy: {{ .root.Values.global.image.pullPolicy | default "IfNotPresent" }}
{{- end -}}
{{- end -}}

{{/*
A kludge to ensure mounted storage (e.g. for nfs, minikube or docker for desktop) has correct permissions for PV
*/}}
{{- define "hpcc.changeMountPerms" -}}
# This is a bit of a hack, to ensure that the persistent storage mounted is writable.
# This is only required when mounting a remote filing systems from another container or machine.
# NB: this includes where the filing system is on the containers host machine .
# Examples include, minikube, docker for desktop, or NFS mounted storage.
# NB: uid=999 and gid=1000 are the uid/gid of the hpcc user, built into platform-core
{{- $permCmd := printf "chown -R 999:1000 %s" .volumePath }}
- name: volume-mount-hack
  image: busybox
  command: [
             "sh",
             "-c",
             "{{ $permCmd }}"
           ]
  volumeMounts:
    - name: {{ .volumeName | quote}}
      mountPath: {{ .volumePath | quote }}
{{- end }}

{{/*
Check dll mount point, using hpcc.changeMountPerms
*/}}
{{- define "hpcc.checkDllMount" -}}
{{- if .root.Values.storage.dllStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "dllstorage-pv" "volumePath" "/var/lib/HPCCSystems/queries") }}
{{- end }}
{{- end }}

{{/*
Check datastorage mount point, using hpcc.changeMountPerms
*/}}
{{- define "hpcc.checkDataMount" -}}
{{- if .root.Values.storage.dataStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "datastorage-pv" "volumePath" "/var/lib/HPCCSystems/hpcc-data") }}
{{- end }}
{{- end }}

{{/*
Check dalistorage mount point, using hpcc.changeMountPerms
*/}}
{{- define "hpcc.checkDaliMount" -}}
{{- if .root.Values.storage.daliStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "dalistorage-pv" "volumePath" "/var/lib/HPCCSystems/dalistorage") }}
{{- end }}
{{- end }}

{{/*
Add any bundles
*/}}
{{- define "hpcc.addBundles" -}}
{{- $in := . -}}
{{- range .root.Values.bundles }}
- name: add-bundle-{{ .name | lower }}
{{ include "hpcc.addImageAttrs" $in | indent 2 }}
  command: [
           "ecl-bundle",
           "install",
           "--remote",
           "{{ .name }}"
           ]
  volumeMounts:
  - name: "hpccbundles"
    mountPath: "/home/hpcc/.HPCCSystems"
{{- end }}
{{- end }}


{{/*
Add security context
Pass in a dictionary with root and me defined
*/}}
{{- define "hpcc.addSecurityContext" -}}
{{- if .root.Values.global.privileged }}
securityContext:
  privileged: true
  capabilities:
    add:
    - SYS_PTRACE
{{- end }}
{{- end -}}


{{/*
Generate instance queue names
*/}}
{{- define "hpcc.generateConfigMapQueues" -}}
{{- range $.Values.eclagent -}}
 {{- if not .disabled -}}
- name: {{ .name }}
  type: {{ .type | default "hthor" }}
  prefix: {{ .prefix | default "null" }}
 {{- end }}
{{ end -}}
{{- range $.Values.roxie -}}
 {{- if not .disabled -}}
- name: {{ .name }}
  type: roxie 
  prefix: {{ .prefix | default "null" }}
 {{- end }}
{{ end -}}
{{- range $.Values.thor -}}
 {{- if not .disabled -}}
- name: {{ .name }}
  type: thor
  prefix: {{ .prefix | default "null" }}
  width: {{ mul (.numSlaves | default 1) ( .channelsPerSlave | default 1) }}
 {{- end }}
{{- end -}}
{{- end -}}
