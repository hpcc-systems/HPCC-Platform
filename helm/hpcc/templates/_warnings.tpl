{{/*

##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

##############################################################################

*/}}
{{/*
Generate a list of warnings
Pass in dict with root and warnings
*/}}
{{- define "hpcc.getWarnings" -}}
 {{- $ctx := . -}}
 {{- $defaultVersion := .root.Values.global.image.version | default .root.Chart.Version -}}
 {{- if (ne $defaultVersion .root.Chart.Version) -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "The expected image version for this helm chart is %s" .root.Chart.Version) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Gather a list of ephemeral and persistant planes */ -}}
 {{- $storage := (.root.Values.storage | default dict) -}}
 {{- $match := dict "ephemeral" (list) "persistant" (list) -}}
 {{- $planes := ($storage.planes | default list) -}}
 {{- $searchLabels := list "data" "dali" "sasha" "dll" "lz" -}}
 {{- range $plane := $planes -}}
  {{- $labels := $plane.labels | default (list "data") -}}
  {{- $tag := (hasKey $plane "storageClass" | ternary "ephemeral" "persistant") -}}
  {{- range $label := $labels -}}
   {{- if has $label $searchLabels -}}
    {{- $prev := get $match $tag -}}
    {{- $_ := set $match $tag (append $prev $plane.name) -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- /* Warn if any of the planes are ephemeral */ -}}
 {{- if (ne (len $match.ephemeral) 0) -}}
  {{ $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "The configuration contains ephemeral planes: %s" (print (uniq $match.ephemeral)) )  -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
{{- end -}}
