This project makes use of the following third-party software:
{{ range . }}
-----------------------------------------------------------------------------
## {{ .Name }}
-----------------------------------------------------------------------------
* Name: {{ .Name }}
* Version: {{ .Version }}
* License: [{{ .LicenseName }}]({{ .LicenseURL }})

A copy of this license is also available in this project at: third_party/licenses
{{ end }}
