{{- /* go-licenses report template for tools/notices.sh: one block per licence file, each library under the file it shares. */ -}}
{{- $path := "" -}}
{{- range . }}
{{- if ne .LicensePath $path }}

================================================================================
{{ .Name }} {{ .Version }} ({{ .LicenseName }})

{{ .LicenseText }}
{{- else }}
{{ .Name }} {{ .Version }} is also under {{ .LicenseName }}, in the text above.
{{- end }}
{{- $path = .LicensePath }}
{{- end }}
