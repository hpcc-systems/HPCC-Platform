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
{{- define "hpcc.utils.generateConfigMapFromFile" -}}
{{- if hasKey .me "configFile" -}}
{{- $filename := (printf "files/%s" .me.configFile) -}}
{{- .me.name -}}.json: |
{{ tpl (.root.Files.Get $filename) .root | indent 2 -}}
{{- else if hasKey .me "config" -}}
{{- .me.name -}}.json: |
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
  global.json: |
    {
      "version": {{ .root.Values.global.image.version | quote }}
    }
{{ include "hpcc.utils.generateConfigMapFromFile" . | indent 2 }}
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

{{- /* Add standard volumes for a component */ -}}
{{- define "hpcc.utils.addVolumes" -}}
volumes:
{{ include "hpcc.utils.addConfigVolume" . }}
- name: dllserver-pv-storage
  persistentVolumeClaim:
    claimName: dllserver-pv-claim
{{- end -}}

{{- /* Add standard volume mounts for a component */ -}}
{{- define "hpcc.utils.addVolumeMounts" -}}
volumeMounts:
{{ include "hpcc.utils.addConfigVolumeMount" . }}
- name: dllserver-pv-storage
  mountPath: "/var/lib/HPCCSystems/queries"
{{- end -}}

{{- /* Add config arg for a component */ -}}
{{- define "hpcc.utils.configArg" -}}
{{- if or (hasKey . "configFile") (hasKey . "config") -}}
"--config=/etc/config/{{ .name }}.json", {{ end -}}
"--global=/etc/config/global.json"
{{- end -}}

{{- /* Add dali arg for a component */ -}}
{{- define "hpcc.utils.daliArg" -}}
"--daliServers={{ (index .Values.dali 0).name }}"
{{- end -}}

{{- /* Add image attributes for a component */ -}}
{{- /* Pass in a dictionary with root and imagename defined */ -}}
{{- define "hpcc.utils.addImageAttrs" -}}
image: "{{ .root.Values.global.image.root | default "hpccsystems" }}/{{ .imagename }}:{{ .root.Values.global.image.version }}"
imagePullPolicy: {{ .root.Values.global.image.pullPolicy }}
{{- end -}}
