{{- define "browser-sandbox.name" -}}
{{- printf "%s-browser" .Release.Name | trunc 51 | trimSuffix "-" -}}
{{- end -}}
{{- define "browser-sandbox.image" -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .Values.image.digest) -}}
{{- fail "image.digest must pin the controller and worker image with sha256:<64 lowercase hex digits>" -}}
{{- end -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end -}}
{{- define "browser-sandbox.labels" -}}
app.kubernetes.io/name: browser-sandbox
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{- define "browser-sandbox.worker-template" -}}
{{- merge (dict "imagePullSecrets" .Values.imagePullSecrets) (omit .Values.worker "disruptionBudget") | toPrettyJson -}}
{{- end -}}
