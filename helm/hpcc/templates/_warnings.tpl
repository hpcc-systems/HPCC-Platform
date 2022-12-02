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
 {{- /* Warn if resources not defined for any cost component or default cpuRate being used */ -}}
 {{- if eq .root.Values.global.cost.perCpu 0.0565000000001 -}}
  {{- $_ := set $ctx "usingDefaultCpuCost" "true" -}}
 {{- end -}}
 {{- $_ := set $ctx "missingResources" list -}}
 {{- $_ := set $ctx "defaultCpuRateComponents" list -}}
 {{- range $component := .root.Values.eclagent -}}
  {{- if not $component.disabled -}}
   {{- $hasResources := include "hpcc.hasResources" (dict "resources" $component.resources) -}}
   {{- if not $hasResources -}}
    {{- $_ := set $ctx "missingResources" (append $ctx.missingResources $component.name) -}}
   {{- end -}}
   {{- if and $ctx.usingDefaultCpuCost (not $component.cost) -}}
    {{- $_ := set $ctx "defaultCpuRateComponents" (append $ctx.defaultCpuRateComponents $component.name) -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- range $component := .root.Values.eclccserver -}}
  {{- if not $component.disabled -}}
   {{- $hasResources := include "hpcc.hasResources" (dict "resources" $component.resources) -}}
   {{- if not $hasResources -}}
    {{- $_ := set $ctx "missingResources" (append $ctx.missingResources $component.name) -}}
   {{- end -}}
   {{- if and $ctx.usingDefaultCpuCost (not $component.cost) -}}
    {{- $_ := set $ctx "defaultCpuRateComponents" (append $ctx.defaultCpuRateComponents $component.name) -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- range $component := .root.Values.thor -}}
  {{- if not $component.disabled -}}
   {{- $hasResources1 := include "hpcc.hasResources" (dict "resources" $component.managerResources) -}}
   {{- $hasResources2 := include "hpcc.hasResources" (dict "resources" $component.workerResources) -}}
   {{- $hasResources3 := include "hpcc.hasResources" (dict "resources" $component.eclAgentResources) -}}
   {{- if or (not $hasResources1) (or (not $hasResources2 ) (not $hasResources3)) -}}
    {{- $_ := set $ctx "missingResources" (append $ctx.missingResources $component.name) -}}
   {{- end -}}
   {{- if and $ctx.usingDefaultCpuCost (not $component.cost) -}}
    {{- $_ := set $ctx "defaultCpuRateComponents" (append $ctx.defaultCpuRateComponents $component.name) -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.missingResources -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Cost calculation requires resources to be provided for %s: %s" ((len $ctx.missingResources)| plural "component" "components") ($ctx.missingResources|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- if $ctx.defaultCpuRateComponents -}}
  {{ $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Default cpu cost rate is being used for %s: %s" ((len $ctx.defaultCpuRateComponents)| plural "component" "components") ($ctx.defaultCpuRateComponents|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if any storage planes uses defaults cost values */ -}}
 {{- with (.root.Values.global.cost) -}}
  {{- if and (eq .storageWrites 0.0500000000001) (and (eq .storageAtRest 0.0208000000001) (eq .storageReads 0.00400000000001)) -}}
   {{- $_ := set $ctx "usingDefaultStorageCosts" "true" -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.usingDefaultStorageCosts -}}
  {{- $_ := set $ctx "planesWithDefaultCosts" list -}}
  {{- range $storagePlane := .root.Values.storage.planes -}}
   {{- if not $storagePlane.disabled -}}
    {{- if not $storagePlane.cost -}}
     {{- $_ := set $ctx "planesWithDefaultCosts" (append $ctx.planesWithDefaultCosts $storagePlane.name) -}}
    {{- else if or (not $storagePlane.cost.storageAtRest) (or (not $storagePlane.cost.storageReads) (not $storagePlane.cost.storageWrites)) -}}
     {{- $_ := set $ctx "planesWithDefaultCosts" (append $ctx.planesWithDefaultCosts $storagePlane.name) -}}
    {{- end -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.planesWithDefaultCosts -}}
   {{- $warning := dict "source" "helm" "severity" "warning" -}}
   {{- $_ := set $warning "msg" (printf "Default cost parameters are being used for the storage %s: %s" ((len $ctx.planesWithDefaultCosts)|plural "plane" "planes") ($ctx.planesWithDefaultCosts|toStrings)) -}}
   {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Report if thor numWorkers is prime */ -}}
 {{- range $thor := .root.Values.thor -}}
  {{- $numWorkers := (int $thor.numWorkers) | default 1 | int -}}
  {{- if gt $numWorkers 2 -}}
   {{- $isPrime := include "hpcc.isPrime" $numWorkers -}}
   {{- if $isPrime -}}
    {{ $warning := dict "source" "helm" "severity" "warning" -}}
    {{- $_ := set $warning "msg" (printf "Thor has prime number of workers (numWorkers %d)" $numWorkers ) -}}
    {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
{{- end -}}
