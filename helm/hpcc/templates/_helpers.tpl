{{/*
Expand the name of the chart.
Pass in dict with root
*/}}
{{- define "hpcc.name" -}}
{{- default .root.Chart.Name .root.Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
Pass in dict with root
*/}}
{{- define "hpcc.fullname" -}}
{{- if .root.Values.fullnameOverride -}}
{{- .root.Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .root.Chart.Name .root.Values.nameOverride -}}
{{- if contains $name .root.Release.Name -}}
{{- .root.Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .root.Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
Pass in dict with root
*/}}
{{- define "hpcc.chart" -}}
{{- printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
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
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $dataStorage := ($storage.dataStorage | default dict) -}}
{{- $spillStorage := ($storage.spillStorage | default dict) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{- $dllStorage := ($storage.dllStorage | default dict) -}}
{{- $dataStoragePlane := ($dataStorage.plane | default "hpcc-data-plane") -}}
{{- $spillStoragePlane := ($spillStorage.plane | default "hpcc-spill-plane") -}}
{{- $daliStoragePlane := ($daliStorage.plane | default "hpcc-dali-plane") -}}
{{- $dllStoragePlane := ($dllStorage.plane | default "hpcc-dlls-plane") -}}
imageVersion: {{ required "Please specify .global.image.version" .Values.global.image.version | quote }}
singleNode: {{ .Values.global.singleNode | default false }}
{{ if .Values.global.defaultEsp -}}
defaultEsp: {{ .Values.global.defaultEsp | quote }}
{{ end -}}
{{ if hasPrefix "[]" (typeOf .Values.esp) -}}
esp:
{{ toYaml .Values.esp }}
{{ end -}}
secretTimeout: {{ .Values.secrets.timeout | default 300 }}
storage:
  daliPlane: {{ $daliStoragePlane }}
  dllsPlane: {{ $daliStoragePlane }}
  dataPlane: {{ $dataStoragePlane }}
  spillPlane: {{ $spillStoragePlane }}
  planes:
{{- /*Generate entries for each data plane (removing the pvc).  Exclude the planes used for dlls and dali.*/ -}}
{{- range $plane := $planes -}}
 {{- if and (ne $plane.name $daliStoragePlane) (ne $plane.name $dllStoragePlane) }}
  - name: {{ $plane.name | quote }}
{{ toYaml (unset (unset (deepCopy $plane) "name") "pvc")| indent 4 }}
 {{- end }}
{{- end }}
{{- /* Add implicit planes if data or spill storage plane not specified*/ -}}
{{- if not $dataStorage.plane }}
  - name: hpcc-data-plane
    mount: {{ .Values.global.defaultDataPath | default "/var/lib/HPCCSystems/hpcc-data" | quote }}
{{- end }}
{{- if not $spillStorage.plane }}
  - name: hpcc-spill-plane
    mount: {{ .Values.global.defaultSpillPath | default "/var/lib/HPCCSystems/hpcc-spill" | quote }}
{{- end }}
{{- if .Values.global.cost }}
cost:
{{ toYaml .Values.global.cost | indent 2 }}
{{- end }}
{{- end -}}

{{/*
Generate local logging info, merged with global
Pass in dict with root and me
*/}}
{{- define "hpcc.generateLoggingConfig" -}}
{{- $logging := deepCopy (.me.logging | default dict) | mergeOverwrite (.root.Values.global.logging | default dict) -}}
{{- if not (empty $logging) }}
logging:
{{ toYaml $logging | indent 2 }}
{{- end -}}
{{- end -}}


{{/*
Add ConfigMap volume mount for a component
*/}}
{{- define "hpcc.addConfigMapVolumeMount" -}}
- name: {{ .name }}-temp-volume
  mountPath: /tmp
- name: {{ .name }}-hpcctmp-volume
  mountPath: /var/lib/HPCCSystems
- name: {{ .name }}-configmap-volume
  mountPath: /etc/config
{{- end -}}

{{/*
Add ConfigMap volume for a component
*/}}
{{- define "hpcc.addConfigMapVolume" -}}
- name: {{ .name }}-temp-volume
  emptyDir: {}
- name: {{ .name }}-hpcctmp-volume
  emptyDir: {}
- name: {{ .name }}-configmap-volume
  configMap:
    name: {{ .name }}-configmap
{{- end -}}

{{/*
Add data volume mount
If any storage planes are defined that name pvcs they will be mounted
*/}}
{{- define "hpcc.addDataVolumeMount" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $dataStorage := ($storage.dataStorage | default dict) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{- $dllStorage := ($storage.dllStorage | default dict) -}}
{{- $daliStoragePlane := ($daliStorage.plane | default "") -}}
{{- $dllStoragePlane := ($dllStorage.plane | default "") -}}
{{- range $plane := $planes -}}
 {{- if $plane.pvc -}}
  {{- if and (ne $plane.name $daliStoragePlane) (ne $plane.name $dllStoragePlane) -}}
   {{- $num := int ( $plane.numDevices | default 1 ) -}}
   {{- if le $num 1 }}
- name: {{ lower $plane.name }}-pv
  mountPath: {{ $plane.prefix | quote }}
   {{- else }}
    {{- range $elem := untilStep 1 (int (add $num 1)) 1 }}
- name: {{ lower $plane.name }}-pv-many-{{- $elem }}
  mountPath: {{ printf "%s/d%d" $plane.prefix $elem | quote }}
    {{- end }}
   {{- end }}
  {{- end }}
 {{- end }}
{{- end }}
{{- if (not $dataStorage.plane) }}
- name: datastorage-pvc
  mountPath: "/var/lib/HPCCSystems/hpcc-data"
{{- end }}
{{- end -}}

{{/*
Add data volume
Pass in dict with root
*/}}
{{- define "hpcc.addDataVolume" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $dataStorage := ($storage.dataStorage | default dict) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{- $dllStorage := ($storage.dllStorage | default dict) -}}
{{- $daliStoragePlane := ($daliStorage.plane | default "") -}}
{{- $dllStoragePlane := ($dllStorage.plane | default "") -}}
{{- range $plane := $planes -}}
 {{- if $plane.pvc -}}
  {{- if and (ne $plane.name $daliStoragePlane) (ne $plane.name $dllStoragePlane) -}}
   {{- $num := int ( $plane.numDevices | default 1 ) -}}
   {{- $pvc := $plane.pvc | required (printf "pvc for %s not supplied" $plane.name) }}
   {{- if le $num 1 }}
- name: {{ lower $plane.name }}-pv
  persistentVolumeClaim:
    claimName: {{ $pvc }}
   {{- else }}
    {{- range $elem := until $num }}
- name: {{ lower $plane.name }}-pv-many-{{- add $elem 1 }}
  persistentVolumeClaim:
    claimName: {{ $pvc }}-{{- add $elem 1 }}
    {{- end }}
   {{- end -}}
  {{- end }}
 {{- end }}
{{- end -}}
{{- if (not $dataStorage.plane) }}
- name: datastorage-pvc
  persistentVolumeClaim:
    claimName: {{ $dataStorage.existingClaim | default (printf "%s-datastorage-pvc" (include "hpcc.fullname" . )) }}
{{- end }}
{{- end -}}

{{/*
Add a volume mount - if default plane is used, or the storage plane specifies a pvc
Pass in dict with root, me, name, and optional path
*/}}
{{- define "hpcc.getVolumeMountPrefix" -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- if .me.plane -}}
 {{- range $plane := $planes -}}
  {{- if and ($plane.pvc) (eq $plane.name .me.plane) -}} # NB: can only be 1 match
{{ $plane.prefix }}
  {{- end -}}
 {{- end -}}
{{- else -}}
{{ printf "/var/lib/HPCCSystems/%s" (.path | default (printf "%sstorage" .name)) | quote }}
{{- end -}}
{{- end -}}

{{/*
Add a volume mount - if default plane is used, or the storage plane specifies a pvc
Pass in dict with root, me, name, and optional path
*/}}
{{- define "hpcc.addVolumeMount" -}}
{{- $mountPath := include "hpcc.getVolumeMountPrefix" . }}
{{- if not $mountPath -}}
{{- $_ := fail (printf "Invalid storage definition for:" .name ) -}}
{{- end -}}
- name: {{ .name }}
  mountPath: {{ $mountPath }}
{{- end -}}

{{/*
Add dll volume mount - if default plane is used, or the dll storage plane specifies a pvc
Pass in dict with root
*/}}
{{- define "hpcc.addDllVolumeMount" -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $dllStorage := ($storage.dllStorage | default dict) -}}
{{ include "hpcc.addVolumeMount" (dict "root" .root "name" "dllstorage-pvc" "path" "queries" "me" $dllStorage) }}
{{- end -}}

{{/*
Add dali volume mount - if default plane is used, or the dali storage plane specifies a pvc
Pass in dict with root
*/}}
{{- define "hpcc.addDaliVolumeMount" -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{ include "hpcc.addVolumeMount" (dict "root" .root "name" "dalistorage-pvc" "path" "dalistorage" "me" $daliStorage) }}
{{- end -}}

{{/*
Add a volume - if default plane is used, or the storage plane specifies a pvc
Pass in dict with root, me and name
*/}}
{{- define "hpcc.addVolume" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- if .me.plane -}}
 {{- range $plane := $planes -}}
  {{- if and ($plane.pvc) (eq $plane.name .me.plane) -}}
- name: {{ .name }}
  persistentVolumeClaim:
    claimName: {{ $plane.pvc }}
  {{- end }}
 {{- end }}
{{- else -}}
- name: {{ .name }}
  persistentVolumeClaim:
    claimName: {{ .me.existingClaim | default (printf "%s-%s" (include "hpcc.fullname" .) .name) }}
{{- end -}}
{{- end -}}

{{/*
Add dll volume - if default plane is used, or the dll storage plane specifies a pvc
Pass in dict with root
*/}}
{{- define "hpcc.addDllVolume" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $dllStorage := ($storage.dllStorage | default dict) -}}
{{ include "hpcc.addVolume" (dict "root" .root "name" "dllstorage-pvc" "me" $dllStorage) }}
{{- end -}}

{{/*
Add dali volume - if default plane is used, or the dali storage plane specifies a pvc
Pass in dict with root
*/}}
{{- define "hpcc.addDaliVolume" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{ include "hpcc.addVolume" (dict "root" .root "name" "dalistorage-pvc" "me" $daliStorage) }}
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
  mountPath: /opt/HPCCSystems/secrets/{{ $category }}/{{ $secretid }}
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
Add sentinel-based probes for a component
*/}}
{{- define "hpcc.addSentinelProbes" -}}
env:
- name: "SENTINEL"
  value: "/tmp/{{ .name }}.sentinel"
startupProbe:
  exec:
    command:
    - cat
    - "/tmp/{{ .name }}.sentinel"
  failureThreshold: 30
  periodSeconds: 10
readinessProbe:
  exec:
    command:
    - cat
    - "/tmp/{{ .name }}.sentinel"
  periodSeconds: 10
{{ end -}}


{{/*
Generate vault info
*/}}
{{- define "hpcc.generateVaultConfig" -}}
{{- $categories := .categories -}}
vaults:
{{- range  $categoryname, $category := .root.Values.vaults -}}
 {{- if (has $categoryname $categories) }}
  {{ $categoryname }}:
  {{- range $vault := . }}
    - name: {{ $vault.name }}
      kind: {{ $vault.kind }}
      url: {{ $vault.url }}
    {{- if index $vault "client-secret" }}
      client-secret: {{ index $vault "client-secret" }}
    {{- end -}}
  {{- end -}}
 {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Return a value indicating whether a storage plane is defined or not.
*/}}
{{- define "hpcc.isValidStoragePlane" -}}
{{- $search := .search -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $dataStorage := ($storage.dataStorage | default dict) -}}
{{- /* If storage.dataStorage.plane is defined, the implicit plane hpcc-dataplane is not defined */ -}}
{{- $done := dict "matched" (and (not $dataStorage.plane) (eq $search "hpcc-dataplane")) -}}
{{- range $plane := $planes -}}
 {{- if eq $search $plane.name -}}
 {{- $_ := set $done "matched" true -}}
 {{- end -}}
{{- end -}}
{{- $done.matched | ternary "true" "false" -}}
{{- end -}}

{{/*
Check that the storage and spill planes for a component exist
*/}}
{{- define "hpcc.checkDefaultStoragePlane" -}}
{{- if (hasKey .me "storagePlane") }}
 {{- $search := .me.storagePlane -}}
 {{- if ne (include "hpcc.isValidStoragePlane" (dict "search" $search "root" .root)) "true" -}}
  {{- $_ := fail (printf "storage data plane %s for %s is not defined" $search .me.name ) }}
 {{- end -}}
{{- end }}
{{- if (hasKey .me "spillPlane") }}
 {{- $search := .me.spillPlane -}}
 {{- if ne (include "hpcc.isValidStoragePlane" (dict "search" $search "root" .root)) "true" -}}
  {{- $_ := fail (printf "storage spill plane %s for %s is not defined" $search .me.name ) }}
 {{- end -}}
{{- end }}
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
# NB: uid=10000 and gid=10001 are the uid/gid of the hpcc user, built into platform-core
{{- $permCmd := printf "chown -R 10000:10001 %s" .volumePath }}
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
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "dllstorage-pvc" "volumePath" "/var/lib/HPCCSystems/queries") }}
{{- end }}
{{- end }}

{{/*
Check datastorage mount point, using hpcc.changeMountPerms
Pass in a dictionary with root
*/}}
{{- define "hpcc.checkDataMount" -}}
{{- if .root.Values.storage.dataStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "datastorage-pvc" "volumePath" "/var/lib/HPCCSystems/hpcc-data") }}
{{- end }}
{{- end }}

{{/*
Check dalistorage mount point, using hpcc.changeMountPerms
*/}}
{{- define "hpcc.checkDaliMount" -}}
{{- if .root.Values.storage.daliStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "dalistorage-pvc" "volumePath" "/var/lib/HPCCSystems/dalistorage") }}
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
{{- define "hpcc.addSecurityContext" }}
securityContext:
{{- if .root.Values.global.privileged }}
  privileged: true
  capabilities:
    add:
    - SYS_PTRACE
  readOnlyRootFilesystem: false
{{- else }}
  capabilities:
    drop:
    - ALL
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
{{- end }}
  runAsNonRoot: true
  runAsUser: 10000
  runAsGroup: 10001
{{ end -}}

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
  queriesOnly: true
 {{- end }}
{{ end -}}
{{- range $.Values.thor -}}
 {{- if not .disabled -}}
- name: {{ .name }}
  type: thor
  prefix: {{ .prefix | default "null" }}
  width: {{ mul (.numWorkers | default 1) ( .channelsPerWorker | default 1) }}
 {{- end }}
{{ end -}}
{{- end -}}

{{/*
Generate list of available services
*/}}
{{- define "hpcc.generateConfigMapServices" -}}
{{- range $roxie := $.Values.roxie -}}
 {{- if not $roxie.disabled -}}
  {{- range $service := $roxie.services -}}
   {{- if ne (int $service.port) 0 -}}
- name: {{ $service.name }}
  type: roxie
  port: {{ $service.port }}
  target: {{ $roxie.name }}
   {{- end -}}
  {{- end }}
{{ end -}}
{{- end -}}
{{- range $esp := $.Values.esp -}}
- name: {{ $esp.name }}
  type: {{ $esp.application }}
  port: {{ $esp.servicePort }}
  tls: {{ $esp.tls }}
  public: {{ $esp.public }}
{{ end -}}
{{- range $dali := $.Values.dali -}}
{{- range $sasha := $dali.services -}}
{{- if and (not $sasha.disabled) ($sasha.servicePort) -}}
- name: {{ printf "sasha-%s" $sasha.service }}
  type: sasha
  port: {{ $sasha.servicePort }}
{{ end -}}
{{ end -}}
{{ end -}}
{{- range $sasha := $.Values.sasha -}}
{{- if and (not $sasha.disabled) ($sasha.servicePort) -}}
- name: {{ printf "sasha-%s" $sasha.service }}
  type: sasha
  port: {{ $sasha.servicePort }}
{{ end -}}
{{ end -}}
{{- end -}}

{{/*
Add resource object
Pass in a dictionary with me defined
*/}}
{{- define "hpcc.addResources" }}
{{- if .me  }}
resources:
  limits:
{{ toYaml .me | indent 4 }}
{{- end }}
{{- end -}}

{{/*
Add resources object for stub pods
*/}}
{{- define "hpcc.addStubResources" }}
resources:
  limits:
    cpu: "50m"
    memory: "100M"
{{- end -}}

{{/*
Generate vault info
*/}}
{{- define "hpcc.generateEclccSecurity" -}}
{{- with .Values.security -}}
{{- if not (empty .eclSecurity) -}}
{{- toYaml (deepCopy .) }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Sasha configmap
Pass in dict with root and me
*/}}
{{- define "hpcc.sashaConfigMap" -}}
{{- $serviceName := printf "sasha-%s" .me.service -}}
apiVersion: v1
metadata:
  name: {{ printf "%s-configmap" $serviceName }}
data:
  {{ $serviceName }}.yaml: |
    version: 1.0
    sasha:
{{ toYaml (omit .me "logging") | indent 6 }}
{{- include "hpcc.generateLoggingConfig" . | indent 6 }}
{{- $categories := list "system" -}}
{{- if has "data" .me.access -}}
{{- $categories := append $categories "storage" -}}
{{- end }}
{{ include "hpcc.generateVaultConfig" (dict "root" .root "categories" $categories ) | indent 6 }}
{{- if .me.storage }}
      storagePath: {{ include "hpcc.getVolumeMountPrefix" (dict "root" .root "name" (printf "sasha-%s" .me.service) "me" .me.storage) }}
{{- end }}
    global:
{{ include "hpcc.generateGlobalConfigMap" .root | indent 6 }}
{{- end -}}

{{/*
A template to generate Sasha service containers
*/}}
{{- define "hpcc.addSashaContainer" }}
{{- $serviceName := printf "sasha-%s" .me.service }}
- name: {{ $serviceName | quote }}
  workingDir: /var/lib/HPCCSystems
  command: [ saserver ] 
  args: [
{{- with (dict "name" $serviceName) }}
          {{ include "hpcc.configArg" . }},
{{- end }}
          "--service={{ .me.service }}",
{{ include "hpcc.daliArg" .root | indent 10 }}
        ]
{{- include "hpcc.addResources" (dict "me" .me.resources) | indent 2 }}
{{- include "hpcc.addSecurityContext" .  | indent 2 }}
{{- with (dict "name" $serviceName) }}
{{ include "hpcc.addSentinelProbes" .  | indent 2 }}
{{- end }}
{{ include "hpcc.addImageAttrs" .  | indent 2 }}
{{- end -}}

{{/*
A template to generate Sasha service
Pass in dict with root, me
*/}}
{{- define "hpcc.addSashaVolumeMounts" }}
{{- $serviceName := printf "sasha-%s" .me.service -}}
{{- if .me.storage }}
{{ include "hpcc.addVolumeMount" (dict "root" .root "name" $serviceName "me" .me.storage ) -}}
{{- end }}
{{ with (dict "name" $serviceName ) -}}
{{ include "hpcc.addConfigMapVolumeMount" . }}
{{- end }}
{{- if has "dalidata" .me.access }}
{{ include "hpcc.addDaliVolumeMount" . -}}
{{- end }}
{{- if has "data" .me.access }}
{{ include "hpcc.addDataVolumeMount" . }}
{{- end }}
{{- if has "dll" .me.access }}
{{ include "hpcc.addDllVolumeMount" . -}}
{{- end }}
{{ include "hpcc.addSecretVolumeMounts" (dict "root" .root "categories" (list "system" ) ) -}}
{{- if has "data" .me.access -}}
{{ include "hpcc.addSecretVolumeMounts" (dict "root" .root "categories" (list "storage" ) ) -}}
{{- end }}
{{- end -}}


{{/*
A template to generate Sasha service
Pass in dict with root, me
*/}}
{{- define "hpcc.addSashaVolumes" }}
{{- $serviceName := printf "sasha-%s" .me.service -}}
{{- if .me.storage }}
{{ include "hpcc.addVolume" (dict "root" .root "name" $serviceName "me" .me.storage) -}}
{{- end }}
{{ with (dict "name" $serviceName) -}}
{{ include "hpcc.addConfigMapVolume" . }}
{{- end }}
{{- if has "dalidata" .me.access }}
{{ include "hpcc.addDaliVolume" . -}}
{{- end }}
{{- if has "data" .me.access }}
{{ include "hpcc.addDataVolume" . }}
{{- end }}
{{- if has "dll" .me.access }}
{{ include "hpcc.addDllVolume" . -}}
{{- end }}
{{ include "hpcc.addSecretVolumes" (dict "root" .root "categories" (list "system" ) ) -}}
{{- if has "data" .me.access -}}
{{ include "hpcc.addSecretVolumes" (dict "root" .root "categories" (list "storage" ) ) -}}
{{- end }}
{{- end -}}

{{/*
A template to generate Sasha service
Pass in dict me
*/}}
{{- define "hpcc.addSashaService" }}
{{- $serviceName := printf "sasha-%s" .me.service }}
apiVersion: v1
kind: Service
metadata:
  name: {{ $serviceName | quote }}
spec:
  ports:
  - port: {{ .me.servicePort }}
    protocol: TCP
    targetPort: {{ .port | default 8877 }}
  selector:
    run: {{ $serviceName | quote }}
  type: ClusterIP
{{- end -}}


{{/*
Return access permssions for a given service
*/}}
{{- define "hpcc.getSashaServiceAccess" }}
{{- if (eq "coalescer" .service) -}}
dalidata
{{- else if (eq "wu-archiver" .service) -}}
dali data dll
{{- else if (eq "dfuwu-archiver" .service) -}}
dali
{{- else if (eq "dfurecovery-archiver" .service) -}}
dali
{{- else if (eq "file-expiry" .service) -}}
dali data
{{- else -}}
{{- $_ := fail (printf "Unknown sasha service:" .service ) -}}
{{- end -}}
{{- end -}}

{{/*
A template to generate a PVC
Pass in dict with root, me, name, and optional path
*/}}
{{- define "hpcc.addPVC" }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ printf "%s-%s" (include "hpcc.fullname" .) .name }}
  labels:
    app.kubernetes.io/name: {{ printf "%s-%s" (include "hpcc.fullname" .) .name }}
    app.kubernetes.io/instance: {{ .root.Release.Name }}
    app.kubernetes.io/managed-by: {{ .root.Release.Service }}
    helm.sh/chart: {{ include "hpcc.chart" . }}
spec:
  accessModes:
    - {{ .mode | default "ReadWriteMany" }}
  resources:
    requests:
      storage: {{ .me.storageSize }}
{{- if .me.storageClass }}
{{- if (eq "-" .me.storageClass) }}
  storageClassName: ""
{{- else }}
  storageClassName: "{{ .me.storageClass }}"
{{- end }}
{{- end }}
{{- end -}}
