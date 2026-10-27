{{- define "kelvo.name" -}}{{ .Release.Name | trunc 40 | trimSuffix "-" }}{{- end -}}

{{- define "kelvo.labels" -}}
app.kubernetes.io/name: kelvo
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "kelvo.selector" -}}
app.kubernetes.io/name: kelvo
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kelvo.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{- define "kelvo.secret" -}}
{{- required "secrets.existingSecret is required (this chart never creates secrets)" .Values.secrets.existingSecret -}}
{{- end -}}

{{- define "kelvo.tlsSecret" -}}
{{- required "tls.existingSecret is required (this chart never creates secrets)" .Values.tls.existingSecret -}}
{{- end -}}

{{- define "kelvo.natsURL" -}}
{{- if .Values.nats.bundled -}}
tls://{{ include "kelvo.name" . }}-nats:4222
{{- else -}}
{{ required "nats.external.url is required when nats.bundled is false" .Values.nats.external.url }}
{{- end -}}
{{- end -}}

{{- define "kelvo.replicas" -}}
{{- if .Values.nats.bundled }}1{{ else }}{{ .Values.nats.external.replicas }}{{ end -}}
{{- end -}}

{{/* Tenant policy shared verbatim by gateway, cluster-init and nodes. */}}
{{- define "kelvo.policy" -}}
tenant_id: {{ .Values.tenant.id | quote }}
max_queries: {{ .Values.tenant.maxQueries }}
job_ttl: {{ .Values.tenant.jobTTL }}
lease_duration: {{ .Values.tenant.leaseDuration }}
replicas: {{ include "kelvo.replicas" . }}
limits:
{{ toYaml .Values.tenant.limits | indent 2 }}
workers:
{{- range .Values.nodes }}
  {{ .id }}: {{ .slots | default 1 }}
{{- end }}
{{- end -}}

{{/* Gateway-side config; $user and $passwordEnv select the NATS credential. */}}
{{- define "kelvo.gatewayConfig" -}}
{{- $ := index . 0 -}}{{- $user := index . 1 -}}{{- $pw := index . 2 -}}
listen: ":8443"
tls:
  cert_file: /run/kelvo/tls/api.crt
  key_file: /run/kelvo/tls/api.key
worker_tls:
  cert_file: /run/kelvo/tls/gateway-client.crt
  key_file: /run/kelvo/tls/gateway-client.key
  ca_file: /run/kelvo/tls/ca.crt
max_queries: {{ $.Values.gateway.maxQueries }}
max_concurrent: {{ $.Values.gateway.maxConcurrent }}
max_http_requests: {{ $.Values.gateway.maxHTTPRequests }}
tenants:
  - token_env: KELVO_TENANT_TOKEN
    policy:
{{ include "kelvo.policy" $ | indent 6 }}
    nats:
      url: {{ include "kelvo.natsURL" $ }}
      username: {{ $user }}
      password_env: {{ $pw }}
      ca_file: /run/kelvo/tls/ca.crt
    workers:
{{- range $.Values.nodes }}
      - {id: {{ .id }}, url: "https://{{ include "kelvo.name" $ }}-node-{{ .id }}:8443"}
{{- end }}
{{- end -}}

{{- define "kelvo.podSecurity" -}}
securityContext:
{{ toYaml .Values.podSecurityContext | indent 2 }}
{{- end -}}
