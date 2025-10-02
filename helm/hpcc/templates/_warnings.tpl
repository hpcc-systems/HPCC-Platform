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
 {{- if hasKey $storage "indexBuildPlane" -}}
  {{- include "hpcc.checkPlaneExists" (dict "root" .root "planeName" $storage.indexBuildPlane "contextPrefix" "indexBuildPlane: ") -}}
 {{- end -}}
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
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "The configuration contains ephemeral planes: %s" (print (uniq $match.ephemeral)) )  -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if any storage planes uses default cost values */ -}}
 {{- if or (eq (float64 .root.Values.global.cost.storageWrites) 0.0500000000001) (or (eq (float64 .root.Values.global.cost.storageAtRest) 0.0208000000001) (eq (float64 .root.Values.global.cost.storageReads) 0.00400000000001)) -}}
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
  {{- if $ctx.planesWithDefaultCosts -}}
   {{- $warning := dict "source" "helm" "severity" "warning" -}}
   {{- $_ := set $warning "msg" (printf "Default cost parameters are being used for the storage %s: %s" ((len $ctx.planesWithDefaultCosts)|plural "plane" "planes") ($ctx.planesWithDefaultCosts|toStrings)) -}}
   {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
  {{- end -}}
 {{- end -}}
 {{- /* Warn if any tracing flags are set in Roxie */ -}}
 {{- range $roxie := .root.Values.roxie -}}
  {{- if not $roxie.disabled -}}
   {{- range $key, $value := $roxie -}}
    {{- if or (eq "traceLevel" $key) (eq "udpTraceLevel" $key) -}}
     {{- if gt (int $value) 1 -}}
      {{- $warning := dict "source" "helm" "severity" "warning" -}}
      {{- $_ := set $warning "msg" (printf "Roxie %s tracing option %s: %s may result in excessive logging" $roxie.name $key (toString $value)) -}}
      {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
     {{- end -}}
    {{- else if or (hasPrefix "trace" $key) (hasPrefix "udpTrace" $key) -}}
     {{- if $value -}}
      {{- $warning := dict "source" "helm" "severity" "warning" -}}
      {{- $_ := set $warning "msg" (printf "Roxie %s tracing option %s: %s may result in excessive logging" $roxie.name $key (toString $value)) -}}
      {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
     {{- end -}}
    {{- end -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- /* Warn if any expert flags are set */ -}}
 {{- range $cname, $ctypes := .root.Values -}}
  {{- range $id, $component := $ctypes -}}
   {{- if kindIs "map" $component -}}
    {{- if not $component.disabled -}}
     {{- range $key, $value := $component.expert -}}
      {{- if $value -}}
       {{- $warning := dict "source" "helm" "severity" "warning" -}}
       {{- $_ := set $warning "msg" (printf "%s %s expert option %s: %s is set" (title $cname) $component.name $key (toString $value)) -}}
       {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
      {{- end -}}
     {{- end -}}
    {{- end -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- /* Warn when resources not provided, default cpu rate used and components requiring resources for cost calcs */ -}}
 {{- if eq (float64 .root.Values.global.cost.perCpu) 0.0565000000001 -}}
  {{- $_ := set $ctx "usingDefaultCpuCost" "true" -}}
 {{- end -}}
 {{- $_ := set $ctx "missingResources" list -}}
 {{- $_ := set $ctx "missingResourcesForCosts" list -}}
 {{- $_ := set $ctx "defaultCpuRateComponents" list -}}
 {{- $_ := set $ctx "components" (pick .root.Values "dafilesrv" "dali" "sasha" "dfuserver" "eclagent" "eclccserver" "esp" "roxie" "thor" "eclscheduler") -}}
 {{- $sasha := .root.Values.sasha | dict -}}
 {{- if ($sasha.disabled|default false) -}}
  {{- $_ := set $ctx "components" (omit $ctx.components "sasha") -}}
 {{- end -}}
 {{- range $cname, $ctypes := $ctx.components -}}
  {{- range $id, $component := $ctypes -}}
   {{- if (kindIs "map" $component) -}}
    {{- if (not $component.disabled) -}}
     {{- $hasResources := "" -}}
     {{- if eq $cname "thor" -}}
      {{- $hasResources = include "hpcc.hasResources" (dict "resources" $component.managerResources) -}}
      {{- $hasResources = (eq $hasResources "true") | ternary (include "hpcc.hasResources" (dict "resources" $component.workerResources)) $hasResources -}}
      {{- $hasResources = (eq $hasResources "true") | ternary (include "hpcc.hasResources" (dict "resources" $component.eclAgentResources)) $hasResources -}}
     {{- else -}}
      {{- $hasResources = include "hpcc.hasResources" (dict "resources" $component.resources) -}}
     {{- end -}}
     {{- if not $hasResources -}}
      {{- $_ := set $ctx "missingResources" (append $ctx.missingResources ($component.name | default $id)) -}}
     {{- end -}}
     {{- /* Checks related to components that are used for cost reporting */ -}}
     {{- /* (n.b. cpuRate ignored for components other than thor, eclagent and eclccserver)*/ -}}
     {{- if has $cname (list "thor" "eclagent" "eclccserver") -}}
      {{- if and $ctx.usingDefaultCpuCost (not $component.cost) -}}
       {{- $_ := set $ctx "defaultCpuRateComponents" (append $ctx.defaultCpuRateComponents $component.name) -}}
      {{- end -}}
      {{- /* Components that are used for cost reporting require resources: warn if resources missing*/ -}}
      {{- if not $hasResources -}}
       {{- $_ := set $ctx "missingResourcesForCosts" (append $ctx.missingResourcesForCosts ($component.name | default $id)) -}}
      {{- end -}}
     {{- end -}}
    {{- end -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.missingResources -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Resources are missing for %s: %s" ((len $ctx.missingResources)| plural "component" "components") ($ctx.missingResources|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- if $ctx.missingResourcesForCosts -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Cost calculation requires resources to be provided for %s: %s" ((len $ctx.missingResourcesForCosts)| plural "component" "components") ($ctx.missingResourcesForCosts|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- if $ctx.defaultCpuRateComponents -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Default cpu cost rate is being used for %s: %s" ((len $ctx.defaultCpuRateComponents)| plural "component" "components") ($ctx.defaultCpuRateComponents|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if insecure embed, pipe or extern enabled */ -}}
 {{- $_ := set $ctx "insecureEclFeature" list -}}
 {{- range $opt, $value := (pick .root.Values.security.eclSecurity "embedded" "pipe" "extern") -}}
  {{- if eq $value "allow" -}}
   {{- $_ := set $ctx "insecureEclFeature" (append $ctx.insecureEclFeature $opt) -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.insecureEclFeature -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Insecure feature enabled in ecl: %s " $ctx.insecureEclFeature) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if thor has both top-level cost and sub-component cost configurations */ -}}
 {{- $_ := set $ctx "thorCostConflicts" list -}}
 {{- $_ := set $ctx "thorInconsistentCosts" list -}}
 {{- $_ := set $ctx "thorUsingCost" list -}}
 {{- range $thor := .root.Values.thor -}}
  {{- if not $thor.disabled -}}
   {{- if $thor.cost -}}
    {{- $_ := set $ctx "thorUsingCost" (append $ctx.thorUsingCost $thor.name) -}}
    {{- $conflictingCosts := list -}}
    {{- if and $thor.manager $thor.manager.cost -}}
     {{- $conflictingCosts = append $conflictingCosts "manager.cost" -}}
    {{- end -}}
    {{- if and $thor.worker $thor.worker.cost -}}
     {{- $conflictingCosts = append $conflictingCosts "worker.cost" -}}
    {{- end -}}
    {{- if and $thor.eclagent $thor.eclagent.cost -}}
     {{- $conflictingCosts = append $conflictingCosts "eclagent.cost" -}}
    {{- end -}}
    {{- if $conflictingCosts -}}
     {{- $_ := set $ctx "thorCostConflicts" (append $ctx.thorCostConflicts (printf "%s (%s)" $thor.name (join ", " $conflictingCosts))) -}}
    {{- end -}}
   {{- end -}}
   {{- /* Check for inconsistent sub-component cost configurations */ -}}
   {{- $managerHasCost := and $thor.manager $thor.manager.cost -}}
   {{- $workerHasCost := and $thor.worker $thor.worker.cost -}}
   {{- $eclagentHasCost := and $thor.eclagent $thor.eclagent.cost -}}
   {{- $anyCost := or $managerHasCost (or $workerHasCost $eclagentHasCost) -}}
   {{- if $anyCost -}}
    {{- $missingCosts := list -}}
    {{- if not $managerHasCost -}}
     {{- $missingCosts = append $missingCosts "manager" -}}
    {{- end -}}
    {{- if not $workerHasCost -}}
     {{- $missingCosts = append $missingCosts "worker" -}}
    {{- end -}}
    {{- if not $eclagentHasCost -}}
     {{- $missingCosts = append $missingCosts "eclagent" -}}
    {{- end -}}
    {{- if $missingCosts -}}
     {{- $_ := set $ctx "thorInconsistentCosts" (append $ctx.thorInconsistentCosts (printf "%s (missing: %s)" $thor.name (join ", " $missingCosts))) -}}
    {{- end -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.thorCostConflicts -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Thor component vs sub-component cost conflict: %s" (join "; " $ctx.thorCostConflicts)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- if $ctx.thorInconsistentCosts -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Thor inconsistent costs: %s" (join "; " $ctx.thorInconsistentCosts)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if thor.cost is being used, but only if "no cost conflicts" hasn't already been reported */ -}}
 {{- if and $ctx.thorUsingCost (not $ctx.thorCostConflicts) -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "Default thor.cost is deprecated (subcomponent level cost config should be provided): %s" ($ctx.thorUsingCost|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if TLS not enabled */ -}}
 {{- $_ := set $ctx "TLSdisabled" list -}}
 {{- range $espservice := .root.Values.esp -}}
  {{- if not $espservice.tls -}}
   {{- if (and ($ctx.root.Values.certificates|default false) $ctx.root.Values.certificates.enabled) -}}
    {{- $externalCert := (ne (include "hpcc.isVisibilityPublic" (dict "root" $ctx.root "visibility" $espservice.service.visibility)) "") -}}
    {{- $externalIssuerKeyName := ternary "remote" "public" (eq "true" ( include "hpcc.usesRemoteIssuer" $espservice )) -}}
    {{- $issuerKeyName := ternary $externalIssuerKeyName "local" $externalCert -}}
    {{- if ne (include "hpcc.isIssuerEnabled" (dict "root" $ctx.root "issuerKeyName" "local")) "true" -}}
     {{- $_ := set $ctx "TLSdisabled" (append $ctx.TLSdisabled $espservice.name) -}}
    {{- end -}}
   {{- else -}}
    {{- $_ := set $ctx "TLSdisabled" (append $ctx.TLSdisabled $espservice.name) -}}
   {{- end -}}
  {{- end -}}
 {{- end -}}
 {{- if $ctx.TLSdisabled -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" (printf "tls disabled for esp services: %s" ($ctx.TLSdisabled|toStrings)) -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if kubeApiCidr/kubeApiPort not provided */ -}}
 {{- $egress := (.root.Values.global.egress | default dict) -}}
 {{- if not (and (hasKey $egress "kubeApiCidr") (hasKey $egress "kubeApiPort")) -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" "egress control to API not restricted (kubeApiCidr/kubeApiPort not provided)" -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- $egressRestricted := $egress.restricted | default false -}}
 {{- if not $egressRestricted -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" "egress is not restricted to minimum required" -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- if .root.Values.global.privileged -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" "privileged access should only be enabled for development systems" -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
 {{- /* Warn if logaccess not provided */ -}}
 {{- if not (hasKey .root.Values.global "logAccess") -}}
  {{- $warning := dict "source" "helm" "severity" "warning" -}}
  {{- $_ := set $warning "msg" "Global LogAccess not configured! Contact admin to enable logs in Zap reports and log viewers" -}}
  {{- $_ := set $ctx "warnings" (append $ctx.warnings $warning) -}}
 {{- end -}}
{{- end -}}
