{{/*
Common naming + label helpers. Mirrors the canonical Helm chart pattern.
*/}}

{{- define "agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent.fullname" -}}
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

{{- define "agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "agent.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "agent.labels" -}}
app.kubernetes.io/name: {{ include "agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Resolve the image tag, defaulting to chart's appVersion if not pinned.
*/}}
{{- define "agent.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Resolve the credential Secret name. If existingSecret is set, use it;
otherwise the chart creates a Secret named "<release>-creds".
*/}}
{{- define "agent.credentialSecretName" -}}
{{- if .Values.ranger.auth.existingSecret -}}
{{- .Values.ranger.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-creds" (include "agent.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
True when basic auth is being configured (either inline or via existing secret).
Only meaningful when auth.mode = basic.
*/}}
{{- define "agent.authConfigured" -}}
{{- if and (eq (default "basic" .Values.ranger.auth.mode) "basic") (or .Values.ranger.auth.existingSecret (and .Values.ranger.auth.principal .Values.ranger.auth.credential)) -}}
true
{{- end -}}
{{- end -}}

{{/*
True when Kerberos auth is selected.
*/}}
{{- define "agent.kerberosEnabled" -}}
{{- if eq (default "basic" .Values.ranger.auth.mode) "kerberos" -}}
true
{{- end -}}
{{- end -}}
