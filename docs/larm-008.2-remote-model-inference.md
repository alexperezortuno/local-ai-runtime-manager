# LARM-008.2 — Remote Model Inference

Inferencia remota autenticada sobre el listener LAN de LARM, reutilizando el proxy OpenAI-compatible existente.

## Estado

PASS

## 1. Contratos inspeccionados antes del cambio

| Archivo | Contrato real verificado |
|---|---|
| `internal/api/lan.go` | El router LAN era una allowlist de una sola ruta (`GET /api/v1/lan/health`). La autenticación comparaba el SHA-256 del Bearer con `crypto/subtle.ConstantTimeCompare` sobre hashes guardados en memoria. La CIDR se evaluaba sobre `r.RemoteAddr` con `net.ParseCIDR`. |
| `internal/api/handler.go` | El router local registra `/v1/models`, `/v1/chat/completions` y `/v1/embeddings` **solo cuando hay Scheduler**. El `Handler` posee el `Manager`, el `Scheduler`, el `Inference Registry`, el `Recovery Service`, el `Planner` y el cliente HTTP del backend. |
| `internal/api/proxy.go` | `serveInference` es el mecanismo único de ejecución: resolución de modelo (`h.manager.Models()`), lease automático (`RequestWithContext`), espera FIFO (`WaitActive`), `BeginInference`, asignación de proceso verificada, proxy a `llama-server` y liberación (`ReleaseAutomatic` / `Release`). El proxy **no** propaga `Authorization` al backend: solo fija `Content-Type` y `Accept`. |
| `internal/scheduler/scheduler.go` | `RequestWithContext`, `WaitActive`, `BeginInference`, `ReleaseAutomatic` y `Release` son las únicas entradas de lease. `SetReleaseProtector` y `SetAdmissionGate` enlazan el Scheduler con el `Inference Registry`. |
| `internal/inference/registry.go` | `Reserve` asigna la identidad de petición y devuelve `ErrRequestIDCollision` ante una colisión. `ProtectLease` impide liberar una lease con inferencia viva. `MarkDownstreamDisconnected` y `RequestCancellation` registran la desconexión. |
| `internal/api/proxy.go` (SSE) | El streaming copia cabecera a cabecera, escribe y hace `Flush` por chunk, y vigila `r.Context().Done()` en tres puntos del bucle para la cancelación por desconexión. El timeout de inactividad (`StreamIdleTimeout`) solo cancela la inferencia; no hay timeout de escritura. |
| `internal/recovery/service.go` | Recovery bloquea la admisión con `ErrRecoveryRequired` a través del `AdmissionGate` del Scheduler. |
| `internal/capacity/admission.go` + `internal/scheduler/admission.go` | Capacity Admission se aplica dentro del Scheduler (`ErrAdmissionCapacity`, `ErrAdmissionTimeout`, `ErrAdmissionUnavailable`). Un evaluador preventivo reutiliza la rama de conflicto existente. |
| `internal/executiongate/coordinator.go` | El Execution Gate es externo al proxy HTTP: interviene en el arranque del proceso del backend, no en la ruta de la petición. |
| `cmd/larm/main.go` | Ambos listeners se construyen en el mismo proceso. El cierre es secuencial: listener local, listener LAN, socket administrativo, Scheduler, Runtime y `MarkCleanShutdown`. |
| `internal/api/proxy_test.go`, `internal/api/lan_test.go` | Los tests del proxy usan un backend falso controlado por `LARM_API_BACKEND_MODE` (`error`, `slow`, `hangstream`, `truncated`). `apiTestScheduler` construye un Manager y un Scheduler reales sobre un binario falso. No se carga ningún GGUF real. |

## 2. Arquitectura

El listener LAN **no** crea un proxy, un Scheduler, un registro de modelos ni una ruta de gestión de procesos propios. El listener LAN y el listener local comparten **la misma instancia de `*api.Handler`**, es decir, el mismo `Scheduler`, el mismo `Runtime Manager`, el mismo `Inference Registry`, el mismo `Recovery Service`, el mismo `Planner` y el mismo cliente HTTP del backend.

```
                ┌──────────────────── proceso LARM ────────────────────┐
                │                                                      │
  LAN (TLS) ──► │  api.newLANHandler  ──┬─► allowlist de rutas        │
                │  ├─ Bearer + CIDR     │                            │
                │  │                    └─► Handler.openAIModels      │
                │  │                    └─► Handler.chatCompletions   │
                │  │                    └─► Handler.embeddings        │
                │  │                             │                    │
  local ──────► │  │                             ▼                    │
                │  │                    Handler.serveInference          │
                │  │                             │                    │
                │  │                             ▼                    │
                │  └──────────► Scheduler ──► Runtime Manager ──► llama-server
                │               (FIFO, leases,        (proceso)       (puerto local)
                │                admission, gate)
                │                     │
                │                     ▼
                │              Inference Registry
                └──────────────────────────────────────────────────────┘
```

El router LAN tiene dos capas:

1. **Allowlist estricta** (`newLANHandler`): un `switch` sobre método y ruta exacta decide si la petición llega al `ServeMux`. Cualquier otra ruta, método alternativo, barra final, doble barra o prefijo equivalente devuelve `404` **antes** de tocar el mux.
2. **Guardia de autenticación y autorización**: Bearer válido y dirección de origen dentro de la allowlist CIDR. Se ejecuta antes de cualquier handler capaz de crear una lease o iniciar un modelo.

## 3. Rutas LAN finales

| Método | Ruta | Estado | Autenticación |
|---|---|---|---|
| `GET` | `/api/v1/lan/health` | Permitido (LARM-008.1) | Bearer + CIDR |
| `GET` | `/v1/models` | Permitido (nuevo) | Bearer + CIDR |
| `POST` | `/v1/chat/completions` | Permitido (nuevo) | Bearer + CIDR |
| `POST` | `/v1/embeddings` | Permitido (nuevo) | Bearer + CIDR |
| *cualquier otro* | *cualquier otra* | `404 not found` | — |

Rutas administrativas comprobadas como inaccesibles desde LAN: `/api/v1/status`, `/api/v1/models`, `/api/v1/processes`, `/api/v1/leases`, `/api/v1/recovery/*`, `/api/v1/capacity/*`, `/api/v1/admission/*`, `/api/v1/models/{id}/start`, `/api/v1/models/{id}/stop`, `/v1/responses`, además de `/api/v1/inferences`, `/api/v1/queue`, `/health` y sus variantes con barra final, doble barra y métodos alternativos (`GET`/`POST`/`PUT`/`PATCH`/`DELETE`/`HEAD`/`OPTIONS`).

## 4. Flujo de autenticación

1. La petición entra en la allowlist de rutas. Si método y ruta no coinciden exactamente, la respuesta es `404` sin tocar la autenticación ni el Scheduler.
2. `Authorization: Bearer <clave>` se compara en tiempo constante contra los hashes SHA-256 de las claves cargadas desde `server.lan.auth.api_keys_env`. Sin cabecera o con clave inválida: `401` con `WWW-Authenticate: Bearer`.
3. La dirección de origen (`r.RemoteAddr`) se resuelve y se comprueba contra los CIDR de `server.lan.access.allow`. Fuera de la allowlist: `403 forbidden`.
4. Solo después de los pasos 2 y 3 se invoca el handler del proxy.

Contratos adicionales:

- Una query string **nunca** autentica: `/v1/models?api_key=...` devuelve `401`.
- El Bearer LAN **no** se reenvía a `llama-server`. El proxy solo fija `Content-Type` y `Accept` en la petición al backend; `Authorization` nunca se copia. El Bearer autentica al cliente ante LARM y nada más.
- Las claves no se registran: solo se guardan sus hashes SHA-256 en memoria y el cuerpo de error es un mensaje uniforme sin material sensible.
- `X-LARM-Lease-ID` y `X-LARM-Request-ID` suministrados por el cliente LAN se descartan (`lanInference`). Las leases y las identidades de petición solo existen en el listener local, de modo que un cliente LAN no puede usar una lease ajena ni colisionar con la identidad de una petición local. El `X-LARM-Execution-ID` se conserva y lo valida el proxy compartido.

## 5. Flujo de leases

El flujo es **idéntico** al de una petición local, porque es literalmente el mismo código (`Handler.serveInference`):

1. Resolución del modelo contra `h.manager.Models()`. Un modelo inexistente devuelve `404`.
2. `h.scheduler.RequestWithContext(queueCtx, requestID, model)` — lease **automática**, con el contexto de espera acotado por `proxy.queue_wait_timeout` y por el plazo del llamante.
3. `h.inferences.Create(...)` — registro durable de la inferencia; si falla, `ProtectLease` impide la liberación prematura.
4. `h.scheduler.WaitActive(queueCtx, leaseID)` — espera FIFO en la cola. Un timeout de espera devuelve `504`.
5. `h.scheduler.BeginInference(...)` — fija el `cancel` de la inferencia sobre la lease.
6. Verificación de la identidad del proceso y `AssignProcess`.
7. Proxy HTTP a `llama-server` en `model.Host:model.Port`.
8. Liberación en el `defer`: `ReleaseAutomatic` si la inferencia fue aceptada, `Release` en caso contrario.

Capacity Admission, Recovery y Execution Gate intervienen en los mismos puntos que en el listener local: la admisión preventiva y la espera de capacidad viven dentro del Scheduler; Recovery bloquea la adquisición con `ErrRecoveryRequired`; el Execution Gate actúa en el arranque del proceso del backend.

No se modificó el Scheduler, ni Recovery, ni Capacity Admission, ni la semántica de liberación de leases. La única adaptación es de transporte: el cliente LAN no puede elegir lease ni identidad de petición.

## 6. Streaming y cancelación

El comportamiento SSE es el del proxy existente, sin cambios:

- `Content-Type: text/event-stream` propagado desde el backend, más `Cache-Control: no-cache` y `X-Accel-Buffering: no`.
- Eventos escritos y `Flush`ados progresivamente: el bucle lee en bloques de 32 KiB, escribe y hace flush en cada iteración. **No** se almacena la respuesta completa en memoria antes de enviarla.
- Terminación normal: el EOF del backend se registra como `SSEEOF` con el resultado del parser SSE.
- Propagación de errores: un backend no-SSE produce `502`; un estado no-2xx del backend se reenvía tal cual con su cuerpo.
- Cancelación por desconexión: `r.Context().Done()` se comprueba antes de leer, después de leer, después de escribir y después del flush. La desconexión marca `MarkDownstreamDisconnected` y cancela la inferencia, lo que libera la lease por el contrato vigente.
- No se introdujeron timeouts de escritura. El único temporizador es el de inactividad de stream (`stream_idle_timeout`), preexistente, que cancela la inferencia y no la escritura.

## 7. Ejemplos curl

```bash
export LARM_CA_FILE=/path/to/larm-tls/larm-ca-cert.pem
export LARM_LAN_KEY='<clave de operador>'
export LARM_LAN='https://192.168.1.20:8443'

# Comprobación de salud autenticada
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_KEY" \
  "$LARM_LAN/api/v1/lan/health"

# Listado de modelos
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_KEY" \
  "$LARM_LAN/v1/models"

# Chat completion
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"model-1","messages":[{"role":"user","content":"hola"}]}' \
  "$LARM_LAN/v1/chat/completions"

# Embeddings
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"model-1","input":"hola"}' \
  "$LARM_LAN/v1/embeddings"

# Streaming SSE
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"model-1","stream":true,"messages":[{"role":"user","content":"hola"}]}' \
  "$LARM_LAN/v1/chat/completions"
```

La verificación TLS se mantiene activa en todos los ejemplos: `--cacert` con la CA privada. No se recomienda `-k` ni `--insecure`.

## 8. Limitaciones

- No se implementa la Responses API: `/v1/responses` devuelve `404` desde LAN.
- No se implementa el listen de modelos remotos ni la descarga de GGUF desde LAN.
- No hay soporte de claves por cliente ni por alcance: existe una única lista de Bearer compartida para las tres rutas de inferencia.
- No hay recarga dinámica de claves ni de allowlist: un cambio exige reiniciar LARM.
- `/v1/models` devuelve el catálogo de modelos configurados, no el estado de residencia ni el estado administrativo de los procesos.
- Un cliente LAN no puede usar una lease explícita ni fijar la identidad de la petición.
- La caché de respuestas, los tokens de uso y las extensiones proprietary de llama.cpp no se interpretan: el proxy solo reenvía el cuerpo del backend.

## 9. Riesgos residuales

- **Allowlist por CIDR sin identidad de host.** La pertenencia a la red es la única prueba de origen, más el secreto compartido. Un atacante con la clave y acceso a la red permitida pasa la verificación. La custodia y rotación de la clave quedan fuera de LARM.
- **Clave compartida única.** La rotación requiere reiniciar LARM y coordinating la distribución entre clientes. No hay revocación granular ni lista de revocación dinámica.
- **Sin límite por cliente.** Todos los clientes LAN compiten por el mismo FIFO del Scheduler y la misma capacidad. Un cliente puede saturar la cola: el Scheduler responde `429` cuando la cola está llena, y ese límite es global, no por cliente.
- **La superficie LAN comparte el Scheduler con la local.** Un cliente LAN puede retrasar a un cliente local ocupando la cola y el modelo. Es el comportamiento pedido —un único mecanismo de ejecución— pero es un riesgo de denegación de servicio dentro de la LAN autorizada.
- **El cuerpo de la petición se reenvía tal cual al backend.** Un cliente LAN puede enviar campos arbitrarios que llama-server interprete. La validación del backend es responsabilidad de llama-server.
- **La inferencia queda registrada en el registro compartido.** El `Inference Registry` guarda los registros de inferencia de peticiones LAN junto a las locales; los payloads nunca se almacenan, pero los metadatos sí. La superficie de lectura de esas inferencias sigue siendo exclusivamente local.
- **Sin TLS de cliente (mTLS).** La autenticación es de una vía. El tráfico LAN está cifrado, pero no autenticado mutualmente.
- **No se ejecutaron pruebas físicas LAN.** No se abrieron puertos de firewall, no se cargaron modelos reales y no se validó el comportamiento con un llama-server real ni con hardware de red real.

## 10. Matriz de pruebas

Archivo: `internal/api/lan_inference_test.go`. Todas las pruebas usan el router LAN real sobre TLS (`httptest.NewTLSServer`), un backend falso controlado por `LARM_API_BACKEND_MODE` y un `Manager` + `Scheduler` reales sobre un binario falso. No se carga ningún GGUF real.

| # | Escenario | Test | Resultado |
|---|---|---|---|
| 1 | `GET /v1/models` autorizado | `TestLANInferenceModelsAuthorized` | PASS |
| 2 | `POST /v1/chat/completions` autorizado | `TestLANInferenceChatAndEmbeddingsAuthorized` | PASS |
| 3 | `POST /v1/embeddings` autorizado | `TestLANInferenceChatAndEmbeddingsAuthorized` | PASS |
| 4 | Solicitudes sin autenticación | `TestLANInferenceRequiresAuthentication` | PASS |
| 5 | Credenciales inválidas | `TestLANInferenceRequiresAuthentication` | PASS |
| 6 | Credencial por query string | `TestLANInferenceRejectsQueryParameterToken` | PASS |
| 7 | IP fuera de la allowlist | `TestLANInferenceRejectsAddressOutsideAllowlist` | PASS |
| 8 | Rutas administrativas denegadas | `TestLANAdministrativeRoutesAreDenied` | PASS |
| 9 | Métodos alternativos, prefijos y barras | `TestLANAlternativeMethodsAreDenied` | PASS |
| 10 | Modelo inexistente | `TestLANUnknownModelIsNotFound` | PASS |
| 11 | Streaming SSE | `TestLANStreamingSSEIsPreserved` | PASS |
| 12 | Streaming progresivo (sin buffer) | `TestLANStreamingDeliversProgressively` | PASS |
| 13 | Desconexión del cliente | `TestLANClientDisconnectReleasesLease` | PASS |
| 14 | Cancelación durante QUEUED | `TestLANQueuedCancellationRemovesAutomaticLease` | PASS |
| 15 | Error de llama-server propagado | `TestLANBackendErrorIsPropagated` | PASS |
| 16 | Reutilización de Scheduler y leases | `TestLANReusesTheSameSchedulerAndLeaseContract` | PASS |
| 17 | Concurrencia | `TestLANConcurrentRequestsShareTheScheduler` | PASS |
| 18 | Lease e identidad de petición del cliente descartados | `TestLANClientSuppliedLeaseIsIgnored` | PASS |
| 19 | Sin exposición directa de llama-server | `TestLANDoesNotExposeLlamaServerDirectly` | PASS |
| 20 | El Bearer no llega al backend | `TestLANDoesNotForwardTheBearerKeyToTheBackend` | PASS |
| 21 | TLS obligatorio | `TestLANUsesTLS` | PASS |
| 22 | La credencial no aparece en la respuesta | `TestLANCredentialIsNotLogged` | PASS |
| 23 | Compatibilidad del listener local | `TestLANLocalListenerRemainsCompatible` | PASS |
| 24 | Shutdown limpio | `TestLANShutdownIsClean` | PASS |
| 25 | Health LAN conservado | `TestLANHealthStillWorks` | PASS |
| 26 | Handler LAN sin proxy (contrato 008.1) | `TestLANListenerWithoutProxyKeepsOnlyTheHealthRoute` | PASS |
| 27 | Autenticación concurrente | `TestLANHandlerConcurrentAuthentication` (preexistente) | PASS |
| 28 | Bind duplicado distinguido | `TestLANBindFailureIsDistinct` (preexistente) | PASS |

Escenarios del contrato de leases verificados sin modificar su semántica: inferencia normal (2, 3, 16), streaming completo (11), desconexión (13), timeout de espera (cubierto por la suite de regresión del proxy local), error de llama-server (15), modelo inexistente (10), concurrencia (17), cancelación durante QUEUED (14) y finalización durante ACTIVE (11, 13, 16).

## 11. Archivos modificados

| Archivo | Cambio |
|---|---|
| `internal/api/lan.go` | Allowlist de rutas LAN ampliada con las tres rutas OpenAI; guardia Bearer + CIDR aplicada a todas las rutas; adaptador `lanInference` que descarta la lease y la identidad de petición del cliente; constructores `NewLANHandlerWithProxy` y `NewLANHandlerWithProxyAndKeys`. |
| `cmd/larm/main.go` | El listener LAN se construye sobre la misma instancia de `*api.Handler` que el listener local, mediante `api.NewLANHandlerWithProxy`. |
| `internal/api/handler_test.go` | El backend falso devuelve la cabecera `Authorization` que recibió, para poder probar que la credencial LAN no se reenvía. |
| `internal/api/lan_inference_test.go` | Nuevo. Suite HTTP sobre el router LAN real con TLS. |
| `docs/larm-008.2-remote-model-inference.md` | Nuevo. Este documento. |

No se modificó el esquema YAML de LARM-008.1, el Scheduler, Recovery, Capacity Admission, el Execution Gate ni el comportamiento del listener local.

## 12. Resultados de las pruebas de regresión

| Comando | Exit code |
|---|---|
| `go test ./... -count=1` | `0` |
| `go test -race ./... -count=1` | `0` |
| `go vet ./...` | `0` |
| `git diff --check` | `0` |

No hubo fallos preexistentes que documentar.
