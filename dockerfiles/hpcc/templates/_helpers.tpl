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

{{- /* Generate local config info into config section */ -}}
{{- /* Pass in a dictionary with root and me defined */ -}}
{{- define "hpcc.generateComponentConfigMap" -}}
{{- if hasKey .me "configFile" -}}
{{- $filename := (printf "files/%s" .me.configFile) -}}
{{- .me.name -}}.yaml: |
{{ tpl (.root.Files.Get $filename) .root | indent 2 -}}
{{- else if hasKey .me "config" -}}
{{- .me.name -}}.yaml: |
{{ .me.config | indent 2 -}}
{{- end -}}
{{- end -}}

{{- /* Generate a ConfigMap for a component */ -}}
{{- /* Pass in a dictionary with root and me defined */ -}}
{{- define "hpcc.generateConfigMap" }}
kind: ConfigMap 
apiVersion: v1 
metadata:
  name: {{ .me.name }}-configmap 
data:
  global.yaml: |
    version: "1.0"
    Global:
      imageVersion: {{ .root.Values.global.image.version | quote }}
      singleNode: {{ .root.Values.global.singleNode }}
{{ include "hpcc.generateComponentConfigMap" . | indent 2 }}
{{ end -}}

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
{{- if or (hasKey . "configFile") (hasKey . "config") -}}
"--config=/etc/config/{{ .name }}.yaml", {{ end -}}
"--global=/etc/config/global.yaml"
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
