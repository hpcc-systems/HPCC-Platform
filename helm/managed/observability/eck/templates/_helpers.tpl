{{/*
Expand the name of the chart.
*/}}
{{- define "eck4hpccobservability.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}
