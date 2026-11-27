{{- define "browser-sandbox.name" -}}
{{- $name := printf "%s-browser" .Release.Name | trunc 51 | trimSuffix "-" -}}
{{- $dnsLabel := "^[a-z]([a-z0-9-]*[a-z0-9])?$" -}}
{{- if or (not (regexMatch $dnsLabel .Release.Name)) (not (regexMatch $dnsLabel $name)) -}}
{{- fail "Release.Name and its truncated resource name must be DNS labels (lowercase letters, digits and hyphens, starting with a letter and ending with a letter or digit)" -}}
{{- end -}}
{{- $name -}}
{{- end -}}
{{- define "browser-sandbox.image" -}}
{{- $repository := required "image.repository is required" (trim (.Values.image.repository | default "")) -}}
{{- if .Values.image.digest -}}
  {{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .Values.image.digest) -}}
    {{- fail "image.digest must use sha256:<64 lowercase hex digits>" -}}
  {{- end -}}
  {{- printf "%s@%s" $repository .Values.image.digest -}}
{{- else -}}
  {{- $tag := .Values.image.tag | default "" -}}
  {{- if not (regexMatch "^(v[0-9]+\\.[0-9]+\\.[0-9]+|sha-[a-f0-9]{40})$" $tag) -}}
    {{- fail "set image.digest or use image.tag with vX.Y.Z or sha-<40 lowercase hex digits>" -}}
  {{- end -}}
  {{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}
{{- define "browser-sandbox.labels" -}}
app.kubernetes.io/name: browser-sandbox
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{- define "browser-sandbox.worker-template" -}}
{{- $profile := .Values.worker.securityContext.seccompProfile -}}
{{- if not (or (eq $profile.type "Unconfined") (and (eq $profile.type "Localhost") $profile.localhostProfile)) -}}
{{- fail "worker.securityContext.seccompProfile must use Unconfined or Localhost with a localhostProfile" -}}
{{- end -}}
{{- merge (dict "imagePullSecrets" .Values.imagePullSecrets) (omit .Values.worker "disruptionBudget") | toPrettyJson -}}
{{- end -}}
