# LARM-008.1 — Native LAN Listener & Authentication

## Contratos inspeccionados antes del cambio

- `server.host` se completa con `127.0.0.1` y solo admite una IP loopback; `server.port` se completa con `8090`.
- El listener local se construye directamente en `cmd/larm/main.go` con `http.Server`, `ReadHeaderTimeout: 5s` y el router completo de `internal/api`.
- Las rutas HTTP se registran en un `http.ServeMux` local. El router incluye estado, modelos, procesos, capacidad, recovery, leases, operaciones OpenAI e inferencias.
- La autenticación existente protege el socket Unix administrativo; no existe autenticación HTTP para el router local.
- El lifecycle local y el administrativo se cerran durante la señal de terminación, antes del scheduler, runtime y recovery.
- Los tests de configuración usan `KnownFields(true)`, por lo que cualquier campo nuevo debe existir explícitamente en el esquema YAML.
- Los tests API locales usan `httptest.NewServer`; no existe aún un harness de lifecycle para dos listeners HTTP.

## Contrato YAML definitivo

```yaml
server:
  host: 127.0.0.1
  port: 8090
  lan:
    enabled: true
    host: 192.168.1.20
    port: 8443
    auth:
      type: bearer
      api_keys_env: LARM_LAN_API_KEYS
    access:
      allow: ["192.168.1.0/24"]
    tls:
      enabled: true
      cert_file: /absolute/path/lan-cert.pem
      key_file: /absolute/path/lan-key.pem
```

Reglas:

- `server.lan` es opcional y `enabled` vale `false` por defecto.
- `host` LAN debe ser una IP no loopback válida; no se acepta `0.0.0.0` porque expone todas las interfaces.
- `port` LAN no puede coincidir con el puerto local ni con el puerto de un modelo.
- `auth.type` debe ser `bearer`; `auth.api_keys_env` nombra una variable de entorno, nunca una clave.
- La variable indicada debe contener una o más claves separadas por comas. Ningún valor se persiste ni se registra.
- `access.allow` es una allowlist CIDR explícita. Una dirección vacía deniega todo.
- `tls.enabled: true` exige `cert_file` y `key_file` absolutos y existentes; la pareja se valida al cargar la configuración. Si `enabled` se omite, los campos de certificado conservan el comportamiento HTTPS anterior.
- `tls.enabled: false` inicia HTTP y no exige certificados. Bearer y la allowlist CIDR siguen activos, pero las credenciales y el tráfico no están cifrados; úsese solo en una red de confianza o con cifrado externo.

## Arquitectura de listeners

- El listener local conserva el router y lifecycle actuales.
- El listener LAN es un `http.Server` separado con un mux nuevo y allowlist explícita.
- Ambos se inician desde `cmd/larm/main.go`; un error de bind LAN se identifica como `LAN HTTP bind` y termina el proceso limpiamente, cerrando cualquier listener ya iniciado.
- La ruta LAN permitida es únicamente `GET /api/v1/lan/health`, autenticada.
- Las demás rutas responden `404`, incluidos admin, inferencias, leases, procesos, configuración, journal, modelos y capacidad.
- El servidor LAN no comparte handler, scheduler, leases, recovery ni capacity admission con la superficie local más allá de la dependencia ya existente del proceso LARM.

## Autenticación y transporte

- Solo `Authorization: Bearer <key>`; una query string nunca autentica.
- Las claves se leen al arrancar y se comparan con `crypto/subtle.ConstantTimeCompare`.
- La respuesta de fallo es `401` con cuerpo JSON uniforme y `WWW-Authenticate: Bearer`.
- Las claves se guardan como hashes SHA-256 en memoria para no conservar el material original; la lista de variables permite rotación futura de múltiples variables/secretos.
- Con TLS activo, TLS 1.2+ se configura directamente en el listener LAN mediante los archivos PEM indicados. Un error de certificado impide iniciar; nunca hay fallback a HTTP.

## Riesgos residuales

- Las IPs de la allowlist se validan por pertenencia CIDR, sin resolver nombres ni controles de identidad de host.
- La rotación requiere reiniciar LARM; no hay recarga dinámica.
- Las claves origen viven en el entorno del proceso; su custodia, permisos y rotación quedan fuera de LARM.
- No se ejecutaron pruebas físicas LAN, apertura de firewall ni inicio de modelos.
