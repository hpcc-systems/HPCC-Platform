{{- /* Translate a port list to a comma-separated list */ -}}
{{- define "hpcc.utils.portListToCommas" -}}
 {{- if hasPrefix "[]" (typeOf .) -}}
  {{- $local := dict "first" true -}}
  {{- range $key, $value := . -}}{{- if not $local.first -}},{{- end -}}{{- $value -}}{{- $_ := set $local "first" false -}}{{- end -}}
 {{- else -}}
  {{- . -}} 
 {{- end -}}
{{- end -}}

{{- /* Generate local config info into config section */ -}}
{{- /* Pass in a dictionary with root and me defined */ -}}
{{- define "hpcc.utils.generateComponentConfigMap" -}}
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
{{- define "hpcc.utils.generateConfigMap" }}
kind: ConfigMap 
apiVersion: v1 
metadata:
  name: {{ .me.name }}-configmap 
data:
  global.yaml: |
    version: "1.0"
    Global:
      imageVersion: {{ .root.Values.global.image.version | quote }}
{{ include "hpcc.utils.generateComponentConfigMap" . | indent 2 }}
{{ end -}}

{{- /* Add a ConfigMap volume for a component */ -}}
{{- define "hpcc.utils.addConfigVolume" -}}
- name: {{ .name }}-configmap-volume
  configMap:
    name: {{ .name }}-configmap
{{- end -}}

{{- /* Add a ConfigMap volume mount for a component */ -}}
{{- define "hpcc.utils.addConfigVolumeMount" -}}
- name: {{ .name }}-configmap-volume
  mountPath: /etc/config
{{- end -}}

{{- /* Add data volume mount for a component */ -}}
{{- define "hpcc.utils.addDataVolumeMount" -}}
- name: datastorage-pv
  mountPath: "/var/lib/HPCCSystems/hpcc-data"
{{- end -}}

{{- /* Add standard volumes for a component */ -}}
{{- define "hpcc.utils.addVolumes" -}}
volumes:
{{ include "hpcc.utils.addConfigVolume" . }}
- name: dllserver-pv-storage
  persistentVolumeClaim:
    claimName: dllserver-pv-claim
- name: datastorage-pv
  persistentVolumeClaim:
    claimName: datastorage-pv-claim
{{- end -}}

{{- /* Add standard volume mounts for a component */ -}}
{{- define "hpcc.utils.addVolumeMounts" -}}
volumeMounts:
{{ include "hpcc.utils.addConfigVolumeMount" . }}
{{ include "hpcc.utils.addDataVolumeMount" . }}
- name: dllserver-pv-storage
  mountPath: "/var/lib/HPCCSystems/queries"
{{- end -}}

{{- /* Add config arg for a component */ -}}
{{- define "hpcc.utils.configArg" -}}
{{- if or (hasKey . "configFile") (hasKey . "config") -}}
"--config=/etc/config/{{ .name }}.yaml", {{ end -}}
"--global=/etc/config/global.yaml"
{{- end -}}

{{- /* Add dali arg for a component */ -}}
{{- define "hpcc.utils.daliArg" -}}
"--daliServers={{ (index .Values.dali 0).name }}"
{{- end -}}

{{- /* Get image name */ -}}
{{- define "hpcc.utils.imageName" -}}
{{- /* Pass in a dictionary with root and imagename defined */ -}}
{{ .root.Values.global.image.root | default "hpccsystems" }}/{{ .imagename }}:{{ .root.Values.global.image.version }}
{{- end -}}

{{- /* Add image attributes for a component */ -}}
{{- /* Pass in a dictionary with root and imagename defined */ -}}
{{- define "hpcc.utils.addImageAttrs" -}}
image: "{{ include "hpcc.utils.imageName" . }}"
imagePullPolicy: {{ .root.Values.global.image.pullPolicy }}
{{- end -}}

{{- /* A kludge to ensure host mounted storage (e.g. for minikube or docker for desktop) has correct permissions for PV */ -}}
{{- define "hpcc.utils.changeMountPerms" -}}
initContainers:
# This is a bit of a hack, to ensure that the persistent storage mounted
# is writable. This is not something we would want to do if using anything other than
# local storage (which is only sensible on single-node systems).
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
