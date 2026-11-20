# LARM-008.5 — Backend Loopback Isolation

## Estado

PASS

## Investigación previa

La validación de `models.*.host` estaba en `internal/config/config.go`, dentro
de `Config.Validate`:

```go
ip := net.ParseIP(model.Host)
if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
    return fmt.Errorf("models.%s.host must be a non-loopback, non-unspecified IP address", id)
}
```

La causa raíz es una validación demasiado restrictiva aplicada al ámbito
equivocado. No existe una dependencia técnica que impida que `llama-server`
escuche en loopback: `net.ParseIP` puede validar esas direcciones y el resto
del flujo ya utiliza el host del modelo.

Los tres ámbitos son independientes:

- `server.host` continúa exigiendo una IP loopback.
- `server.lan.host` continúa exigiendo una IP no loopback y no unspecified,
  además de sus validaciones actuales de TLS, Bearer y CIDR.
- `models.*.host` valida una IP y rechaza únicamente direcciones inválidas o
  unspecified; las direcciones loopback quedan permitidas explícitamente.

La construcción de argumentos en `internal/backend/llamacpp.go` ya utiliza
`model.Host` para `--host` y conserva `127.0.0.1` como default cuando el campo
está vacío. `internal/runtime/manager.go` usa el mismo host para `/health` y
`internal/api/proxy.go` usa el mismo host para el destino HTTP. No se
encontraron direcciones LAN hardcodeadas ni fallback a `0.0.0.0`.

`cmd/larm/main.go` mantiene dos listeners separados: el local, restringido por
`server.host`, y el LAN, validado por `server.lan.host` y protegido por TLS,
Bearer y CIDR. El listener LAN reutiliza el handler/proxy existente; no crea
un segundo proxy ni conecta directamente al puerto de `llama-server`.

## Contrato corregido

```yaml
server:
  host: "127.0.0.1"
  port: 8090
  lan:
    enabled: true
    host: "192.169.0.228"
    port: 8443

models:
  code-agent:
    path: "/models/code-agent.gguf"
    host: "127.0.0.1"
    port: 8060
```

El host de cada modelo es explícito y no se cambia silenciosamente. Si se omite,
el comportamiento existente conserva el default de loopback `127.0.0.1`.

## Arquitectura de red

```text
Cliente remoto
    |
    | HTTPS + Bearer
    v
192.169.0.228:8443
    |
    v
LARM
    |
    | HTTP interno
    v
127.0.0.1:8060
    |
    v
llama-server
```

## Evidencia de construcción y resolución

- `internal/backend/llamacpp.go:Args` conserva el host configurado en
  `--host` y el puerto configurado en `--port`. Con `host: 127.0.0.1` y
  `port: 8060` produce `--host 127.0.0.1 --port 8060`.
- El host vacío mantiene el default existente `127.0.0.1`; no existe fallback a
  `0.0.0.0`.
- `internal/runtime/manager.go` resuelve readiness con el mismo
  `model.Host` y `model.Port` mediante `backend.HealthyAt`; el host vacío usa
  `127.0.0.1`.
- `internal/api/proxy.go` construye el destino con el mismo host y puerto
  mediante `net.JoinHostPort`. El test de proxy usa un backend falso enlazado
  en loopback y recibe la petición correctamente.
- `cmd/larm/main.go` enlaza el listener LAN únicamente con
  `server.lan.host:server.lan.port`; no se usa para construir el backend.

## Pruebas

Las pruebas nuevas cubren:

| Escenario | Test | Resultado |
|---|---|---|
| `models.*.host: 127.0.0.1` aceptado | `TestValidateModelHostAllowsLoopbackAndExistingAddresses` | PASS |
| Otra loopback IPv4 válida aceptada | `TestValidateModelHostAllowsLoopbackAndExistingAddresses` | PASS |
| Dirección inválida y unspecified rechazadas | `TestValidateModelHostRejectsInvalidAndUnspecifiedAddresses` | PASS |
| Configuración anterior no loopback compatible | `TestValidateModelHostAllowsLoopbackAndExistingAddresses` | PASS |
| `server.host` sigue restringido a loopback | `TestValidateServerHostStillRequiresLoopback` | PASS |
| `server.lan.host` sigue rechazando loopback | `TestValidateLANHostStillRejectsLoopback` | PASS |
| Construcción de `--host 127.0.0.1` y `--port 8060` | `TestArgsUsesConfiguredLoopbackHostAndPort` | PASS |
| Health check hacia loopback | `TestHealthyAtUsesConfiguredLoopback` | PASS |
| Proxy hacia backend loopback | `TestProxyUsesConfiguredLoopbackBackend` | PASS |
| Sin fallback wildcard | `TestArgsUsesConfiguredLoopbackHostAndPort` | PASS |
| Parámetros de LARM-009 | Suite existente de `internal/config` y `internal/backend` | PASS |

No se iniciaron modelos GGUF. Los tests usan archivos de modelo de texto y
servidores HTTP falsos.

## Resultados de regresión

| Comando | Exit code |
|---|---:|
| `go test ./... -count=1` | 0 |
| `go test -race ./... -count=1` | 0 |
| `go vet ./...` | 0 |
| `git diff --check` | 0 |

## Archivos modificados

- `internal/config/config.go`: permite loopback solo para `models.*.host`.
- `internal/config/host_test.go`: validación de los tres ámbitos de host.
- `internal/backend/llamacpp_test.go`: argumentos y health check loopback.
- `internal/api/handler_test.go`: fixture explícitamente enlazado a loopback.
- `internal/api/proxy_test.go`: proxy hacia loopback.
- `README.md` y `configs/models.example.yaml`: ejemplos con host de backend
  explícito.
- Este documento.

No se modificaron Scheduler, leases, Recovery, Capacity Admission, Execution
Gate, el proxy HTTP de producción ni la autenticación LAN. No se modificaron
configuraciones operativas, reglas de firewall ni endpoints OpenAI.

## Riesgos residuales

- Permitir un host de modelo no loopback conserva compatibilidad con
  configuraciones existentes; el aislamiento de red depende de configurar
  explícitamente cada backend en loopback.
- La protección frente a clientes remotos sigue siendo el listener LAN
  autenticado y el binding del backend; no se modifican firewall, TLS,
  autenticación ni el proxy HTTP.
- No se realizarán pruebas físicas LAN ni se iniciarán modelos reales.
