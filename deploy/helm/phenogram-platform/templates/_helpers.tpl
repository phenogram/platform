{{- define "phenogram.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "phenogram.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "phenogram.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "phenogram.labels" -}}
helm.sh/chart: {{ include "phenogram.chart" . }}
{{ include "phenogram.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "phenogram.selectorLabels" -}}
app.kubernetes.io/name: {{ include "phenogram.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "phenogram.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "phenogram.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "phenogram.appImage" -}}
{{- if .Values.image.digest -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end -}}
{{- end }}

{{- define "phenogram.telegramImage" -}}
{{- if .Values.telegramBotApi.image.digest -}}
{{ printf "%s@%s" .Values.telegramBotApi.image.repository .Values.telegramBotApi.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.telegramBotApi.image.repository (default .Chart.AppVersion .Values.telegramBotApi.image.tag) }}
{{- end -}}
{{- end }}

{{- define "phenogram.postgresImage" -}}
{{- if .Values.postgresql.image.digest -}}
{{ printf "%s@%s" .Values.postgresql.image.repository .Values.postgresql.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.postgresql.image.repository .Values.postgresql.image.tag }}
{{- end -}}
{{- end }}
