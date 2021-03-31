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
{{- $certificates := (.Values.certificates | default dict) -}}
{{- $issuers := ($certificates.issuers | default dict) -}}
mtls: {{ and ($certificates.enabled) (hasKey $issuers "local") }}
imageVersion: {{ .Values.global.image.version | default .Chart.Version }}
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
  dllsPlane: {{ $dllStoragePlane }}
  dataPlane: {{ $dataStoragePlane }}
  spillPlane: {{ $spillStoragePlane }}
  planes:
{{- /*Generate entries for each data plane (removing the pvc).  Exclude the planes used for dlls and dali.*/ -}}
{{- range $plane := $planes -}}
 {{- if or (not $plane.labels) (or (has "data" $plane.labels) (has "lz" $plane.labels)) }}
  - name: {{ $plane.name | quote }}
{{ toYaml (unset (unset (deepCopy $plane) "name") "pvc")| indent 4 }}
 {{- end }}
{{- end }}
{{- /* Add implicit planes if data or spill storage plane not specified*/ -}}
{{- if not $dataStorage.plane }}
  - name: hpcc-data-plane
    prefix: {{ .Values.global.defaultDataPath | default "/var/lib/HPCCSystems/hpcc-data" | quote }}
{{- end }}
{{- if not $spillStorage.plane }}
  - name: hpcc-spill-plane
    prefix: {{ .Values.global.defaultSpillPath | default "/var/lib/HPCCSystems/hpcc-spill" | quote }}
{{- end }}
{{- if .Values.global.cost }}
cost:
{{ toYaml .Values.global.cost | indent 2 }}
{{- end }}
{{- end -}}

{{/*
Generate dfuserver queues
Pass in root
*/}}
{{- define "hpcc.generateConfigDfuQueues" -}}
{{- range $queue := .root.Values.dfuserver }}
{{- if not $queue.disabled }}
- name: {{ .name }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Generate local logging info, merged with global
Pass in dict with root and me
*/}}
{{- define "hpcc.generateLoggingConfig" -}}
{{- $logging := deepCopy (.me.logging | default dict) | mergeOverwrite dict (.root.Values.global.logging | default dict) -}}
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
Returns a non empty string if any labels in the list includeLabels is in the plane.labels
Note: the list includeLabels may contain an empty string (""), which matches planes that do not have a label
Pass in plane and includeLabels
Return: If there is any matching labels, there will be a non-empty string returned.  If there is no matching labels,
        an empty string will be returned.
*/}}
{{- define "hpcc.doesStorageLabelsMatch" -}}
{{- $plane := .plane -}}
  {{- range $label := .includeLabels -}}
    {{- if and (eq $label "") (not $plane.labels) -}}
      {{- print "T" -}}
    {{- else if has $label $plane.labels -}}
      {{- print "T" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Add volume mounts
Pass in root and includeLabels (optional)
Note: if there are multiple planes (other than dll, dali and spill planes), they should be all called with a single call
to addVolumeMounts so that if a plane can be used for multiple purposes then duplicate volume mounts are not created.
*/}}
{{- define "hpcc.addVolumeMounts" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $dataStorage := ($storage.dataStorage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $includeLabels := .includeLabels | default list -}}
{{- range $plane := $planes -}}
 {{- if $plane.pvc -}}
  {{- $matchedLabels := include "hpcc.doesStorageLabelsMatch" (dict "plane" $plane "includeLabels" $includeLabels) -}}
  {{- if ne $matchedLabels "" }}
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
{{- /*
Create a data volume mount if data plane have not been specified in storage.planes
Note: Some services used addVolumeMounts to add data planes and other types of plane using addVolumeMounts, so this code has
to be located here rather than in addDataVolumeMount.
*/ -}}
{{- if and (has "data" $includeLabels) (not $dataStorage.plane) }}
- name: datastorage
  mountPath: "/var/lib/HPCCSystems/hpcc-data"
{{- end }}
{{- end -}}

{{/*
Add data volume mount
Pass in root
*/}}
{{- define "hpcc.addDataVolumeMount" -}}
{{- include "hpcc.addVolumeMounts" (dict "root" .root "includeLabels" (list "data" "")) -}}
{{- end -}}

{{/*
Add volumes
Pass in root and includeLabels (optional)
*/}}
{{- define "hpcc.addVolumes" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $dataStorage := ($storage.dataStorage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $includeLabels := .includeLabels | default list -}}
{{- range $plane := $planes -}}
 {{- if $plane.pvc -}}
  {{- $matchedLabels := include "hpcc.doesStorageLabelsMatch" (dict "plane" $plane "includeLabels" $includeLabels) -}}
  {{- if ne $matchedLabels "" }}
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
{{- /*
Create a data volume if data plane have not been specified in storage.planes
Note: Some services used addVolumes to add data planes and other types of plane using addVolumes, so this code has
to be located here rather than in addDataVolumes.
*/ -}}
{{- if and (has "data" $includeLabels) (not $dataStorage.plane) }}
- name: datastorage
  persistentVolumeClaim:
    claimName: {{ $dataStorage.existingClaim | default (printf "%s-datastorage" (include "hpcc.fullname" . )) }}
{{- end }}
{{- end -}}

{{/*
Add data volume
Pass in dict with root
*/}}
{{- define "hpcc.addDataVolume" -}}
{{- include "hpcc.addVolumes" (dict "root" .root "includeLabels" (list "data" "") ) -}}
{{- end -}}

{{/*
Add a volume mount - if default plane is used, or the storage plane specifies a pvc
Pass in dict with root, me, name, and optional path
*/}}
{{- define "hpcc.getVolumeMountPrefix" -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- if .me.plane -}}
{{- $me := .me -}}
 {{- range $plane := $planes -}}
  {{- if and ($plane.pvc) (eq $plane.name $me.plane) -}}
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
{{ include "hpcc.addVolumeMount" (dict "root" .root "me" $dllStorage "name" ($dllStorage.plane | default "dllstorage") "path" "queries") }}
{{- end -}}

{{/*
Add dali volume mount - if default plane is used, or the dali storage plane specifies a pvc
Pass in dict with root
*/}}
{{- define "hpcc.addDaliVolumeMount" -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $planes := ($storage.planes | default list) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{ include "hpcc.addVolumeMount" (dict "root" .root "me" $daliStorage "name" ($daliStorage.plane | default "dalistorage") "path" "dalistorage") }}
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
{{- $me := .me -}}
 {{- range $plane := $planes -}}
  {{- if and ($plane.pvc) (eq $plane.name $me.plane) -}}
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
{{ include "hpcc.addVolume" (dict "root" .root "name" "dllstorage" "me" $dllStorage) }}
{{- end -}}

{{/*
Add dali volume - if default plane is used, or the dali storage plane specifies a pvc
Pass in dict with root
*/}}
{{- define "hpcc.addDaliVolume" -}}
{{- /*Create local variables which always exist to avoid having to check if intermediate key values exist*/ -}}
{{- $storage := (.root.Values.storage | default dict) -}}
{{- $daliStorage := ($storage.daliStorage | default dict) -}}
{{ include "hpcc.addVolume" (dict "root" .root "name" "dalistorage" "me" $daliStorage) }}
{{- end -}}

{{/*
Add the secret volume mounts for a component
Pass in dict with root and secretsCategories
*/}}
{{- define "hpcc.addSecretVolumeMounts" -}}
{{- $secretsCategories := .secretsCategories -}}
{{- range $category, $key := .root.Values.secrets -}}
 {{- if (has $category $secretsCategories) -}}
{{- range $secretid, $secretname := $key -}}
- name: secret-{{ $secretid }}
  mountPath: /opt/HPCCSystems/secrets/{{ $category }}/{{ $secretid }}
{{ end -}}
 {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add Secret volume for a component
Pass in dict with root and secretsCategories
*/}}
{{- define "hpcc.addSecretVolumes" -}}
{{- $component := .component -}}
{{- $secretsCategories := .secretsCategories -}}
{{- range $category, $key := .root.Values.secrets -}}
{{- if (has $category $secretsCategories) -}}
{{- range $secretid, $secretname := $key }}
- name: secret-{{ $secretid }}
  secret:
    secretName: {{ $secretname }}
{{- end -}}
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
{{- $secretsCategories := .secretsCategories -}}
vaults:
{{- range  $categoryname, $category := .root.Values.vaults -}}
 {{- if (has $categoryname $secretsCategories) }}
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
{{ .me.image.root | default .root.Values.global.image.root | default "hpccsystems" }}/{{ .me.image.name | default .root.Values.global.image.name | default "platform-core" }}:{{ .me.image.version | default .root.Values.global.image.version | default .root.Chart.Version }}
{{- else -}}
{{ .root.Values.global.image.root | default "hpccsystems" }}/{{ .root.Values.global.image.name | default "platform-core" }}:{{ .root.Values.global.image.version | default .root.Chart.Version }}
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
Container to watch for a file on a shared mount and execute a command
Pass in dict with me and command
NB: an alternative to sleep loop would be to install and make use of inotifywait
*/}}
{{- define "hpcc.addWaitAndRunContainer" -}}
- name: wait-and-run
  image: busybox
  command:
    - sh
    - "-c"
    - |
      /bin/sh <<'EOSCRIPT'
      set -e
      while true; do
        if [ -f /wait-and-run/{{ .me.name }}.jobdone ]; then break; fi
        echo waiting for /wait-and-run/{{ .me.name }}.jobdone
        sleep 5
      done
      echo "Running: {{ .command }}"
      if {{ .command }}; then
        echo "Command succeeded"
      fi
      EOSCRIPT
  volumeMounts:
  - name: wait-and-run
    mountPath: "/wait-and-run"
{{- end }}

{{/*
Add wait-and-run shared inter container mount
*/}}
{{- define "hpcc.addWaitAndRunVolumeMount" -}}
- name: wait-and-run
  mountPath: "/wait-and-run"
{{- end }}

{{/*
Add wait-and-run shared inter container volume
*/}}
{{- define "hpcc.addWaitAndRunVolume" -}}
- name: wait-and-run
  emptyDir: {}
{{- end }}

{{/*
Check dll mount point, using hpcc.changeMountPerms
*/}}
{{- define "hpcc.checkDllMount" -}}
{{- if .root.Values.storage.dllStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "dllstorage" "volumePath" "/var/lib/HPCCSystems/queries") }}
{{- end }}
{{- end }}

{{/*
Check datastorage mount point, using hpcc.changeMountPerms
Pass in a dictionary with root
*/}}
{{- define "hpcc.checkDataMount" -}}
{{- if .root.Values.storage.dataStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "datastorage" "volumePath" "/var/lib/HPCCSystems/hpcc-data") }}
{{- end }}
{{- end }}

{{/*
Check dalistorage mount point, using hpcc.changeMountPerms
*/}}
{{- define "hpcc.checkDaliMount" -}}
{{- if .root.Values.storage.daliStorage.forcePermissions | default false }}
{{ include "hpcc.changeMountPerms" (dict "root" .root "volumeName" "dalistorage" "volumePath" "/var/lib/HPCCSystems/dalistorage") }}
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
  public: {{ $service.external }}
   {{- end -}}
  {{- end }}
{{ end -}}
{{- end -}}
{{- range $esp := $.Values.esp -}}
- name: {{ $esp.name }}
  type: {{ $esp.application }}
  port: {{ $esp.servicePort }}
  {{- if hasKey $esp "tls" }}
  tls: {{ $esp.tls }}
  {{- else }}
  tls: {{ ($.Values.certificates | default dict).enabled }}
  {{- end }}
  public: {{ $esp.public }}
{{ end -}}
{{- range $dali := $.Values.dali -}}
{{- $sashaServices := $dali.services | default dict -}}
{{- if not $sashaServices.disabled -}}
{{- range $sashaName, $_sasha := $sashaServices -}}
{{- $sasha := ($_sasha | default dict) -}}
{{- if and (not $sasha.disabled) ($sasha.servicePort) -}}
- name: {{ printf "sasha-%s" $sashaName }}
  type: sasha
  port: {{ $sasha.servicePort }}
{{ end -}}
{{ end -}}
{{ end -}}
{{ end -}}
{{- $sashaServices := $.Values.sasha | default dict -}}
{{- if not $sashaServices.disabled -}}
{{- range $sashaName, $_sasha := $sashaServices -}}
{{- $sasha := ($_sasha | default dict) -}}
{{- if and (not $sasha.disabled) ($sasha.servicePort) -}}
- name: {{ printf "sasha-%s" $sashaName }}
  type: sasha
  port: {{ $sasha.servicePort }}
{{ end -}}
{{ end -}}
{{- end -}}
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
Pass in dict with root, me and secretsCategories
*/}}
{{- define "hpcc.sashaConfigMap" -}}
{{- $configMapName := printf "sasha-%s" .me.name -}}
apiVersion: v1
metadata:
  name: {{ printf "%s-configmap" $configMapName }}
data:
  {{ $configMapName }}.yaml: |
    version: 1.0
    sasha:
{{ toYaml (omit .me "logging") | indent 6 }}
{{- include "hpcc.generateLoggingConfig" . | indent 6 }}
{{ include "hpcc.generateVaultConfig" . | indent 6 }}
{{- if .me.storage }}
      storagePath: {{ include "hpcc.getVolumeMountPrefix" (dict "root" .root "me" .me.storage "name" (printf "sasha-%s" .me.name) ) }}
{{- end }}
    global:
{{ include "hpcc.generateGlobalConfigMap" .root | indent 6 }}
{{- end -}}

{{/*
A template to generate Sasha service containers
Pass in dict with root, me and dali if container in dali pod
*/}}
{{- define "hpcc.addSashaContainer" }}
{{- $serviceName := printf "sasha-%s" .me.name }}
- name: {{ $serviceName | quote }}
  workingDir: /var/lib/HPCCSystems
  command: [ saserver ] 
  args: [
{{- with (dict "name" $serviceName) }}
          {{ include "hpcc.configArg" . }},
{{- end }}
          "--service={{ .me.name }}",
{{ include "hpcc.daliArg" .root | indent 10 }}
        ]
{{- include "hpcc.addResources" (dict "me" .me.resources) | indent 2 }}
{{- include "hpcc.addSecurityContext" . | indent 2 }}
{{- with (dict "name" $serviceName) }}
{{ include "hpcc.addSentinelProbes" . | indent 2 }}
{{- end }}
{{ include "hpcc.addImageAttrs" (dict "root" .root "me" (.dali | default .me)) | indent 2 }}
{{- end -}}

{{/*
A template to generate Sasha service
Pass in dict with root and me
*/}}
{{- define "hpcc.addSashaVolumeMounts" }}
{{- $serviceName := printf "sasha-%s" .me.name -}}
{{- if .me.storage }}
{{ include "hpcc.addVolumeMount" (dict "root" .root "me" .me.storage "name" (.me.storage.plane | default $serviceName)) -}}
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
{{- end -}}
{{- end }}


{{/*
A template to generate Sasha service
Pass in dict with root and me
*/}}
{{- define "hpcc.addSashaVolumes" }}
{{- $serviceName := printf "sasha-%s" .me.name -}}
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
{{- end -}}

{{/*
A template to generate Sasha service
Pass in dict me
*/}}
{{- define "hpcc.addSashaService" }}
{{- $serviceName := printf "sasha-%s" .me.name }}
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
{{- if (eq "coalescer" .name) -}}
dalidata
{{- else if (eq "wu-archiver" .name) -}}
dali data dll
{{- else if (eq "dfuwu-archiver" .name) -}}
dali
{{- else if (eq "dfurecovery-archiver" .name) -}}
dali
{{- else if (eq "file-expiry" .name) -}}
dali data
{{- else -}}
{{- $_ := fail (printf "Unknown sasha service:" .name ) -}}
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

{{/*
Create placement related settings
Pass in dict with placement
*/}}
{{- define "hpcc.doPlacement" -}}
{{- if .me.placement }}
{{ toYaml .me.placement }}
{{- end -}}
{{- end -}}

{{/*                                                                                                                            Check if there is any placement configuration
Pass in dict with root, job, target and type
*/}}
{{- define "hpcc.placementsByJobTargetType" -}}
{{- if .root.Values.placements }}
{{- $job := .job -}}
{{- $target := (printf "target:%s" .target | default "") -}}
{{- $type := printf "type:%s" .type -}}
{{- range $placement := .root.Values.placements -}}
{{- if or (has $target $placement.pods) (has $type $placement.pods) -}}
{{ include "hpcc.doPlacement" (dict "me" $placement) -}}
{{- else -}}
{{- range $jobPattern := $placement.pods -}}
{{- if mustRegexMatch $jobPattern $job -}}
{{ include "hpcc.doPlacement" (dict "me" $placement) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Check if there is any placement configuration
Pass in dict with root, pod, target and type
*/}}
{{- define "hpcc.placementsByPodTargetType" -}}
{{- if .root.Values.placements }}
{{- $pod := .pod -}}
{{- $target := (printf "target:%s" .target | default "") -}}
{{- $type := printf "type:%s" .type -}}
{{- range $placement := .root.Values.placements -}}
{{- if or (has $pod $placement.pods) (has $target $placement.pods) (has $type $placement.pods) -}}
{{ include "hpcc.doPlacement" (dict "me" $placement) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Generate lifecycle, command and args
Pass in root, me and command
*/}}
{{- define "hpcc.addCommandAndLifecycle" -}}
{{- $misc := .root.Values.global.misc | default dict }}
{{- $postJobCommand := $misc.postJobCommand | default "" }}
{{- if and (not $misc.postJobCommandViaSidecar) $postJobCommand }}
lifecycle:
  preStop:
    exec:
      command:
      - >-
          {{ $postJobCommand }}
{{- end }}
command: ["/bin/bash"]
args:
- -c
- >-
    {{ .command }}
{{- if $misc.postJobCommandViaSidecar -}} ;
    touch /wait-and-run/{{ .me.name }}.jobdone
{{- else if $postJobCommand -}} ;
    {{ $postJobCommand }}
{{- end }}
{{- end -}}

{{/*
Use cert-manager to create a public certificate and private key for use with TLS
There are separate certificate issuers for local and public certificates
by default public certificates are self-signed and local certificates are signed
by our own certificate authority.  A CA certificate is also provided to the pod
so that we can recognize the signature of our own CA.
*/}}
{{- define "hpcc.addCertificate" }}
{{- if (.root.Values.certificates | default dict).enabled -}}
{{- $externalCert := and (hasKey . "external") .external -}}
{{- $issuer := ternary .root.Values.certificates.issuers.public .root.Values.certificates.issuers.local $externalCert -}}
{{- if $issuer -}}
{{- $namespace := .root.Release.Namespace -}}
{{- $service := (.service | default dict) -}}
{{- $domain := ( $service.domain | default $issuer.domain | default $namespace | default "default" ) -}}
{{- $exposure := ternary "public" "local" $externalCert -}}
{{- $name := .name }}

apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ .component }}-{{ $exposure }}-{{ $name }}-cert
  namespace: {{ $namespace }}
spec:
  # Secret names are always required.
  secretName: {{ .component }}-{{ $exposure }}-{{ $name }}-tls
  duration: 2160h # 90d
  renewBefore: 360h # 15d
  subject:
    organizations:
    - HPCC Systems
  commonName: {{ $name }}.{{ $domain }}
  isCA: false
  privateKey:
    algorithm: RSA
    encoding: PKCS1
    size: 2048
  usages:
    - server auth
    - client auth
  dnsNames:
 {{- /* if servicename is passed we simply create a service entry of that name */ -}}
 {{- if .servicename }}
  - {{ .servicename }}.{{ $domain }}
 {{- /* if service parameter is passed in we are using the component config as a service config entry */ -}}
 {{- else if .service -}}
   {{- $public := and (hasKey .service "public") .service.public -}}
   {{- if eq $public $externalCert }}
  - {{ .service.name }}.{{ $domain }}
   {{- end }}
 {{- /* if services parameter is passed the component has an array of services to configure */ -}}
 {{- else if .services -}}
  {{- range $service := .services }}
   {{- $external := and (hasKey $service "external") $service.external -}}
   {{- if eq $external $externalCert }}
  - {{ $service.name }}.{{ $domain }}
   {{- end }}
  {{- end }}
 {{- else if not $externalCert }}
  - "{{ $name }}.{{ $domain }}"
 {{- end }}
  uris:
  - spiffe://hpcc.{{ $domain }}/{{ .component }}/{{ $name }}
  # Issuer references are always required.
  issuerRef:
    name: {{ $issuer.name }}
    # We can reference ClusterIssuers by changing the kind here.
    kind: {{ $issuer.kind }}
    group: cert-manager.io
---
{{- end }}
{{- end }}
{{- end }}

{{/*
Experimental: Use certmanager to generate a key for roxie udp encryption.
A public certificate and private key are generated under /opt/HPCCSystems/secrets/certificates/udp.
Current udp encryption design would only use the private key.
Key is in pem format and the private key would need to be extracted.
*/}}
{{- define "hpcc.addUDPCertificate" }}
{{- if (.root.Values.certificates | default dict).enabled -}}
{{- $issuer := .root.Values.certificates.issuers.local -}}
{{- $namespace := .root.Release.Namespace -}}
{{- $name := .name -}}
{{- if $issuer }}
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ .component }}-udp-{{ $name }}-cert
  namespace: {{ $namespace }}
spec:
  # Secret names are always required.
  secretName: {{ .component }}-udp-{{ $name }}-dtls
  duration: 2160h # 90d
  renewBefore: 360h # 15d
  subject:
    organizations:
    - HPCC Systems
  commonName: {{ $name }}.{{ $namespace }}
  isCA: false
  privateKey:
    algorithm: ECDSA
    encoding: PKCS1
    size: 256
  usages:
    - server auth
    - client auth
  # At least one of a DNS Name, URI, or IP address is required.
  uris:
  - spiffe://hpcc.{{ $namespace }}/{{ .component }}/{{ $name }}
  # Issuer references are always required.
  issuerRef:
    name: {{ $issuer.name }}
    # We can reference ClusterIssuers by changing the kind here.
    # The default value is Issuer (i.e. a locally namespaced Issuer)
    kind: {{ $issuer.kind }}
    group: cert-manager.io
---
{{- end }}
{{- end }}
{{- end }}

{{/*
Add a certficate volume mount for a component
*/}}
{{- define "hpcc.addCertificateVolumeMount" -}}
{{- $externalCert := and (hasKey . "external") .external -}}
{{- $exposure := ternary "public" "local" $externalCert }}
{{- /*
    A .certificate parameter means the user explictly configured a certificate to use
    otherwise check if certificate generation is enabled
*/ -}}
{{- if .certificate -}}
- name: certificate-{{ .component }}-{{ $exposure }}-{{ .name }}
  mountPath: /opt/HPCCSystems/secrets/certificates/{{ $exposure }}
{{- else if (.root.Values.certificates | default dict).enabled -}}
{{- $issuer := ternary .root.Values.certificates.issuers.public .root.Values.certificates.issuers.local $externalCert -}}
{{- if $issuer -}}
- name: certificate-{{ .component }}-{{ $exposure }}-{{ .name }}
  mountPath: /opt/HPCCSystems/secrets/certificates/{{ $exposure }}
{{- end }}
{{- end -}}
{{- end -}}

{{/*
Add a secret volume for a certificate
*/}}
{{- define "hpcc.addCertificateVolume" -}}
{{- $externalCert := and (hasKey . "external") .external -}}
{{- $exposure := ternary "public" "local" $externalCert -}}
{{- /*
    A .certificate parameter means the user explictly configured a certificate to use
    otherwise check if certificate generation is enabled
*/ -}}
{{- if .certificate -}}
- name: certificate-{{ .component }}-{{ $exposure }}-{{ .name }}
  secret:
    secretName: {{ .certificate }}
{{- else if (.root.Values.certificates | default dict).enabled -}}
{{- $issuer := ternary .root.Values.certificates.issuers.public .root.Values.certificates.issuers.local $externalCert -}}
{{- if $issuer -}}
- name: certificate-{{ .component }}-{{ $exposure }}-{{ .name }}
  secret:
    secretName: {{ .component }}-{{ $exposure }}-{{ .name }}-tls
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add the certficate volume mount for a roxie udp key
*/}}
{{- define "hpcc.addUDPCertificateVolumeMount" }}
{{- if (.root.Values.certificates | default dict).enabled -}}
{{- if .root.Values.certificates.issuers.local -}}
- name: certificate-{{ .component }}-udp-{{ .name }}
  mountPath: /opt/HPCCSystems/secrets/certificates/udp
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add a secret volume for a roxie udp key
*/}}
{{- define "hpcc.addUDPCertificateVolume" }}
{{- if (.root.Values.certificates | default dict).enabled -}}
{{- if .root.Values.certificates.issuers.local -}}
- name: certificate-{{ .component }}-udp-{{ .name }}
  secret:
    secretName: {{ .component }}-udp-{{ .name }}-dtls
{{ end -}}
{{- end -}}
{{- end -}}
