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

{{- /* Translate a port list to a comma-separated list */ -}}
{{- define "hpcc.portListToCommas" -}}
 {{- if hasPrefix "[]" (typeOf .) -}}
  {{- $local := dict "first" true -}}
  {{- range $key, $value := . -}}{{- if not $local.first -}},{{- end -}}{{- $value -}}{{- $_ := set $local "first" false -}}{{- end -}}
 {{- else -}}
  {{- . -}} 
 {{- end -}}
{{- end -}}

{{/*
Generate local config info into config section
*/}}
{{- /* Pass in a dictionary with component and me defined */ -}}
{{- define "hpcc.generateComponentConfigMap" -}}
{{- .me.name -}}.yaml: |
  version: 1.0
  {{ .component }}:
{{ toYaml .me | indent 4 }}
{{- end -}}

{{- /* Generate a ConfigMap for a component */ -}}
{{- /* Pass in a dictionary with root, component and me defined */ -}}
{{- define "hpcc.generateConfigMap" -}}
kind: ConfigMap 
apiVersion: v1 
metadata:
  name: {{ .me.name }}-configmap 
data:
  global.yaml: |
    version: "1.0"
    global:
      imageVersion: {{ .root.Values.global.image.version | quote }}
      singleNode: {{ .root.Values.global.singleNode }}
{{ include "hpcc.generateComponentConfigMap" . | indent 2 -}}
{{- end -}}

{{- /* Add a ConfigMap volume for a component */ -}}
{{- define "hpcc.addConfigVolume" -}}
- name: {{ .name }}-configmap-volume
  configMap:
    name: {{ .name }}-configmap
{{- end -}}

{{- /* Add a ConfigMap volume mount for a component */ -}}
{{- define "hpcc.addConfigVolumeMount" -}}
- name: {{ .name }}-configmap-volume
  mountPath: /etc/config
{{- end -}}

{{- /* Add data volume mount for a component */ -}}
{{- define "hpcc.addDataVolumeMount" -}}
- name: datastorage-pv
  mountPath: "/var/lib/HPCCSystems/hpcc-data"
{{- end -}}

{{- /* Add standard volumes for a component */ -}}
{{- define "hpcc.addVolumes" -}}
- name: dllserver-pv-storage
  persistentVolumeClaim:
    claimName: {{ .Values.global.dllserver.existingClaim | default (printf "%s-dllserver-pv-claim" (include "hpcc.fullname" .)) }}
- name: datastorage-pv
  persistentVolumeClaim:
    claimName: {{ .Values.global.dataStorage.existingClaim | default (printf "%s-datastorage-pv-claim" (include "hpcc.fullname" .)) }}
{{- end -}}

{{- /* Add standard volume mounts for a component */ -}}
{{- define "hpcc.addVolumeMounts" -}}
volumeMounts:
{{ include "hpcc.addConfigVolumeMount" . }}
{{ include "hpcc.addDataVolumeMount" . }}
- name: dllserver-pv-storage
  mountPath: "/var/lib/HPCCSystems/queries"
{{- end -}}

{{- /* Add config arg for a component */ -}}
{{- define "hpcc.configArg" -}}
"--config=/etc/config/{{ .name }}.yaml", "--global=/etc/config/global.yaml"
{{- end -}}

{{- /* Add dali arg for a component */ -}}
{{- define "hpcc.daliArg" -}}
"--daliServers={{ (index .Values.dali 0).name }}"
{{- end -}}

{{- /* Get image name */ -}}
{{- define "hpcc.imageName" -}}
{{- /* Pass in a dictionary with root, me and imagename defined */ -}}
{{- if .me.image -}}
{{ .me.image.root | default .root.Values.global.image.root | default "hpccsystems" }}/{{ .imagename }}:{{ .me.image.version | default .root.Values.global.image.version }}
{{- else -}}
{{ .root.Values.global.image.root | default "hpccsystems" }}/{{ .imagename }}:{{ .root.Values.global.image.version }}
{{- end -}}
{{- end -}}

{{- /* Add image attributes for a component */ -}}
{{- /* Pass in a dictionary with root, me and imagename defined */ -}}
{{- define "hpcc.addImageAttrs" -}}
image: {{ include "hpcc.imageName" . | quote }}
{{ if .me.image -}}
imagePullPolicy: {{ .me.image.pullPolicy | default .root.Values.global.image.pullPolicy | default "IfNotPresent" }}
{{- else -}}
imagePullPolicy: {{ .root.Values.global.image.pullPolicy | default "IfNotPresent" }}
{{- end -}}
{{- end -}}

{{- /* A kludge to ensure host mounted storage (e.g. for minikube or docker for desktop) has correct permissions for PV */ -}}
{{- define "hpcc.changeHostMountPerms" -}}
initContainers:
# This is a bit of a hack, to ensure that the persistent storage mounted
# is writable. This is not something we would want to do if using anything other than
# hostPath storage (which is only sensible on single-node systems).
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

{{- /* Check dllserver host mount point, using hpcc.changeHostMountPerms */ -}}
{{- define "hpcc.checkDllServerHostMount" -}}
{{- if .root.Values.global.hostStorage | default false }}
{{ include "hpcc.changeHostMountPerms" (dict "root" .root "volumeName" "dllserver-pv-storage" "volumePath" "/var/lib/HPCCSystems/queries") }}
{{- end }}
{{- end }}

{{- /* Check datastorage host mount point, using hpcc.changeHostMountPerms */ -}}
{{- define "hpcc.checkDataStorageHostMount" -}}
{{- if .root.Values.global.hostStorage | default false }}
{{ include "hpcc.changeHostMountPerms" (dict "root" .root "volumeName" "datastorage-pv" "volumePath" "/var/lib/HPCCSystems/hpcc-data") }}
{{- end }}
{{- end }}

{{- /* Add security context */ -}}
{{- /* Pass in a dictionary with root and me defined */ -}}
{{- define "hpcc.addSecurityContext" -}}
{{- if .root.Values.global.privileged -}}
securityContext:
  privileged: true
  capabilities:
    add:
    - SYS_PTRACE
{{- end -}}
{{- end -}}

