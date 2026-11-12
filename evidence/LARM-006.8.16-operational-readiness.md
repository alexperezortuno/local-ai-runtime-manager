# LARM-006.8.16 — Operational CLI & Readiness

**Fecha:** 2026-11-05  
**Plataforma principal:** macOS arm64  
**Commits:** ninguno  
**Clasificación:** **PASS PARCIAL**

## 1. Resumen ejecutivo

LARM queda utilizable para consulta cotidiana mediante `larm models`, `larm
status`, `larm queue` y `larm doctor`. Las consultas reutilizan los endpoints y
snapshots existentes; no se creó una API paralela. `doctor` es read-only, no
carga modelos y trata un servicio detenido como `WARN`.

La administración mutante continúa exclusivamente en el Unix socket protegido
con peer credentials y capabilities Ed25519. ADM-04 y ADM-16 pasaron con
`llama-server` y un GGUF real en instancias aisladas. Se especificó el error de
capacidad conservando HTTP `409`.

La suite macOS, Race Detector, `vet`, crash real y validaciones focales pasan.
La validación Linux ejecutó los componentes compatibles, pero una fixture
existente (`TestStopAndNewRequestSerializeAcrossStopping`) no observa
`STOPPING` dentro de su ventana en Linux; por ello la fase no se clasifica como
PASS completo.

## 2. Inventario de comandos existentes

Antes de esta fase ya estaban disponibles en `internal/admincli`:

```text
larm admin models
larm admin status
larm admin incidents
larm admin incident INCIDENT_ID
larm admin plan INCIDENT_ID
larm admin processes
larm admin auth status
larm admin auth issue INCIDENT_ID PLAN_ID
larm admin auth issue-model-stop MODEL_ID
larm admin auth inspect CAPABILITY_FILE
larm admin model stop MODEL_ID
larm admin recovery execute
```

Las consultas administrativas siguen devolviendo JSON metadata-only. El Stop
administrativo se conserva sin ruta HTTP alternativa.

## 3. Brechas encontradas

| Funcionalidad | Implementación existente | Brecha | Cambio mínimo |
|---|---|---|---|
| Consulta operativa | Snapshots HTTP de modelos, estado y cola | No había comandos de primer nivel ni salida tabular | Añadir `RunReadOnly` reutilizando el cliente existente |
| Diagnóstico | Validación al arranque y `/health` | No había comprobación agrupada read-only | Añadir `doctor` local sin mutaciones |
| Capacidad | `runtime.ErrCapacity` terminaba en conflicto genérico | Diagnóstico insuficiente | Error Scheduler específico, mismo HTTP `409` |
| Seguridad `path` | Campo histórico en `/api/v1/models` | La CLI no lo necesitaba | Omitirlo en CLI y documentar exposición; no romper contrato |
| Linux | Variantes Linux existentes | No había campaña ejecutada | Ejecutar con imagen Linux preexistente y registrar el bloqueo real |

No se duplicaron endpoints, Scheduler, Recovery, leases ni autorización.

## 4. Cambios implementados

- `cmd/larm` despacha los comandos read-only de primer nivel, incluso con
  flags antes o después del comando.
- `internal/admincli` renderiza modelos, estado y cola con datos existentes:
  estado/residencia, PID, `ProcessInstanceID`, leases, protección y capacidad.
- `larm doctor` comprueba configuración, binario, GGUF, puertos de backend y
  API, directorio de logs, Recovery, servicio y capacidad. No escribe archivos,
  no crea claves, no ejecuta Recovery y no detiene procesos.
- `scheduler.ErrCapacity` produce, cuando la causa es verificablemente una
  plaza ocupada, mensajes como `model capacity exhausted: 1/1 resident slots
  occupied`; los mapeos públicos conservan `409`.
- Se corrigió una lectura de `activeID` fuera del mutex durante `ManualStop`,
  detectada por Race Detector Linux.
- README y documentación de residencia incluyen los comandos reales, Stop
  autorizado, capacidad y riesgo del campo `path`.

## 5. Contratos CLI finales

```text
larm [-config PATH] [-url URL] [-timeout DURATION] models
larm [-config PATH] [-url URL] [-timeout DURATION] status
larm [-config PATH] [-url URL] [-timeout DURATION] queue
larm [-config PATH] [-url URL] [-timeout DURATION] doctor
```

`models` muestra `MODEL`, `STATE`, `RESIDENCY`, `PID`,
`PROCESS_INSTANCE_ID`, `LEASES`, protección y ocupación. `path` no se muestra.

`status` muestra servicio, backend reportado por el runtime, cantidad de
modelos, residentes, capacidad usada/límite, solicitudes en cola y Recovery.

`queue` muestra posición FIFO para pendientes, modelo, estado, lease ID y
espera observada desde `created_at` cuando existe. No inventa contadores del
Scheduler.

`doctor` devuelve filas `PASS`, `WARN`, `FAIL` o `NOT_CHECKED`. Un servicio
detenido produce `WARN`; una configuración inválida impide las comprobaciones
dependientes y produce `NOT_CHECKED`.

La administración sigue siendo:

```text
larm admin -socket SOCKET -private-key KEY -out CAP auth issue-model-stop MODEL
larm admin -socket SOCKET -capability-file CAP model stop MODEL
```

## 6. Matriz de pruebas OPS-01 a OPS-20

| ID | Resultado | Evidencia |
|---|---|---|
| OPS-01 | PASS | Inventario de `internal/admincli` y ayuda actualizada. |
| OPS-02 | PASS | `TestReadOnlyOperationalCommandsRenderExistingSnapshots`; `/api/v1/models` existente. |
| OPS-03 | PASS | Misma prueba CLI; `/api/v1/status` existente. |
| OPS-04 | PASS | Misma prueba CLI; `/api/v1/queue` existente y FIFO. |
| OPS-05 | PASS | Capability Ed25519, socket, scope, replay, auditoría y ADM-04/16. |
| OPS-06 | PASS | `doctor` valida configuración, binario, GGUF, puertos y capacidad. |
| OPS-07 | PASS | `doctor` consulta `/health`; estado detenido clasificado como WARN. |
| OPS-08 | PASS | `TestDoctorIsReadOnlyAndClassifiesStoppedServiceAsWarning`; config permanece intacta. |
| OPS-09 | PASS | `TestPersistentCapacityIsStrict`; mensaje `1/1 resident slots occupied`; HTTP conserva `409`. |
| OPS-10 | PASS | Suite HTTP completa y tests existentes de proxy/leases. |
| OPS-11 | PASS | Campaña GGUF aislada ADM-04, Stop antes de `time_alive=30s`. |
| OPS-12 | PASS | Campaña GGUF aislada ADM-16, SSE real y Stop rechazado durante protección. |
| OPS-13 | PASS | Lease protegida durante SSE; no se fuerza liberación. |
| OPS-14 | PASS | `make test-real-crash`, CRASH-01..08 y survivor no adoptado. |
| OPS-15 | BLOCKED | Linux compatible pasa con `-skip` de una fixture; la fixture de STOPPING falla en Linux. |
| OPS-16 | PASS | Tests OpenAI existentes y validación GGUF real previa con JSON/SSE. |
| OPS-17 | PASS | README, `docs/model-residency.md` e informe actualizados. |
| OPS-18 | PASS | `go test -race -count=1 ./...` pasa en macOS; Linux subset sin race pasa con `-race`. |
| OPS-19 | PASS | `make test-real-crash` pasa en la ejecución final. |
| OPS-20 | PASS | ADM-04/ADM-16 y shutdown aislados sin backend residual propio. |

## 7. Resultado ADM-04 — Stop durante `time_alive`

Instancia aislada:

```text
/var/folders/zr/hnnn53p16qb_vh8tl5c73k2c0000gn/T/larm-006.8.16-real-wt9fz6a6
```

Configuración real: `keep_alive: false`, `time_alive: 30s`, un GGUF
`Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf`, puertos y socket temporales.

Resultado:

- Inferencia real HTTP 200.
- Modelo observado como `READY` con residencia `READY_RETAINED`.
- PID inicial `21778` y ProcessInstanceID
  `924c5362-0e83-46f2-8cad-97acc5d99b1c`.
- Capability válida ejecutó Stop con salida exitosa.
- Estado posterior `STOPPED`; terminación `TERMINATED_VERIFIED`.
- Se cargó una instancia nueva antes del vencimiento original: PID `21785`,
  ProcessInstanceID `1bf12815-f64d-4d9e-8f3e-468172feffe0`.
- Se esperaron 31.46 s respecto al vencimiento original; la nueva instancia
  conservó su ProcessInstanceID y permaneció `READY`.
- Lease liberada, shutdown con código 0 y ausencia de backend residual propio.

**ADM-04: PASS.** El timer antiguo no afectó la instancia posterior.

## 8. Resultado ADM-16 — Stop durante SSE

Instancia aislada:

```text
/var/folders/zr/hnnn53p16qb_vh8tl5c73k2c0000gn/T/larm-006.8.16-real-xt_igwax
```

Resultado:

- SSE real HTTP 200, 12 eventos, primer fragmento observado y EOF normal.
- Durante la transmisión el backend permaneció `READY`, PID `21755`.
- `protected_inference` fue `true`.
- Stop con capability válida fue rechazado con:
  `lease is protected by an active or ambiguous inference: protected inference is unresolved`.
- La inferencia terminó sin error de stream; no se liberó la protección a la
  fuerza.
- Un segundo Stop autorizado después de finalizar la inferencia terminó con
  éxito y el modelo quedó `STOPPED`.
- Shutdown con código 0 y ausencia de backend residual propio.

**ADM-16: PASS.** No se interpretó la desconexión del cliente como finalización
semántica.

## 9. Resultado Linux

Se usó la imagen Linux preexistente `golang:1.26`, con el module cache del host
montado read-only y red deshabilitada; no se instalaron paquetes.

Pasó:

```text
go test ./internal/runtime ./internal/adminauth ./internal/recovery ./internal/scheduler \
  -skip TestStopAndNewRequestSerializeAcrossStopping -count=1
PASS

go test -race ./internal/runtime ./internal/adminauth ./internal/recovery ./internal/scheduler \
  -skip TestStopAndNewRequestSerializeAcrossStopping -count=1
PASS
```

La suite compatible completa quedó bloqueada por:

```text
TestStopAndNewRequestSerializeAcrossStopping
condition was not met before timeout
```

La fixture no observó `STOPPING` en Linux; no se relajó la assertion ni se
modificó Recovery, identidad o terminación para forzar el resultado.

## 10. Diagnósticos

Ejecución real de `larm doctor` sobre la configuración existente, sin arrancar
LARM ni tocarla:

```text
CONFIG        PASS
LLAMA_SERVER  PASS
GGUF          PASS
PORT_18069    PASS
PORT_18070    PASS
PORT_8090     WARN  service is stopped; configured API port is available
LOG_DIR       WARN  no model log_file configured; backend logs use process stderr
RECOVERY_DIR  PASS
CAPACITY      PASS  max_active_models=1
SERVICE       WARN  service is stopped or unavailable
API           WARN  local API connectivity not available
```

No modelo fue cargado y no se ejecutó Recovery.

## 11. Seguridad de información

`GET /api/v1/models` está limitado a la dirección loopback por validación de
configuración, pero no requiere autenticación administrativa. El campo histórico
`path` expone la ruta del archivo GGUF a cualquier cliente local con acceso al
endpoint. La CLI no necesita ese dato y no lo imprime.

El campo no se eliminó para preservar el contrato público. La retirada,
redacción o protección autenticada del campo debe ser una corrección separada.
No se observó exposición de claves, capabilities, tokens o prompts en la salida
CLI implementada.

## 12. Compatibilidad OpenAI

La suite existente de `GET /v1/models`, chat JSON, embeddings y chat SSE pasa.
La evidencia GGUF previa confirma HTTP 200, `choices`, `usage`,
`finish_reason`, `[DONE]` y EOF normal. El cambio de capacidad solo modifica el
mensaje cuando `runtime.ErrCapacity` es verificable y conserva HTTP `409`.

## 13. Race Detector

Resultado final macOS:

```text
go test -race -count=1 ./...  PASS
```

La campaña Linux detectó inicialmente una lectura de `activeID` fuera del mutex
en `ManualStop`; se corrigió tomando la decisión bajo el mutex y usando el valor
capturado después de desbloquear. La suite Linux focal con esa fixture excluida
pasó bajo `-race`.

## 14. Crash real

```text
make test-real-crash  PASS
```

Pasaron CRASH-01..08 y `SURVIVOR-process-is-not-adopted`. No se modificó Recovery
para hacer pasar el escenario.

## 15. Matriz de comandos obligatorios

| Comando | Resultado |
|---|---|
| `go test ./...` | PASS |
| `go test -race -count=1 ./...` | PASS |
| `go vet ./...` | PASS |
| `git diff --check` | PASS |
| `make test-real-crash` | PASS |
| focal CLI/admin/residency | PASS |
| ADM-04 GGUF real | PASS |
| ADM-16 GGUF real | PASS |
| Linux completo | BLOCKED por fixture STOPPING |

## 16. Archivos modificados en esta fase

- `README.md`
- `cmd/larm/main.go`
- `docs/model-residency.md`
- `internal/admincli/cli.go`
- `internal/admincli/cli_test.go`
- `internal/api/handler.go`
- `internal/api/proxy.go`
- `internal/scheduler/scheduler.go`
- `internal/scheduler/residency_test.go`
- `evidence/LARM-006.8.16-operational-readiness.md`

El checkout ya contenía modificaciones de fases anteriores en varios de esos
archivos; no se revirtieron ni se realizaron commits.

## 17. Riesgos pendientes

- La fixture `TestStopAndNewRequestSerializeAcrossStopping` continúa bloqueada
  en Linux y requiere una investigación de compatibilidad de prueba separada.
- No se declara PASS Linux completo para identidad/terminación mientras esa
  fixture no tenga resultado equivalente.
- `GET /api/v1/models.path` mantiene el riesgo de revelar rutas a clientes
  loopback no autenticados.
- No se implementa gestión de RAM/VRAM ni validación de memoria disponible; está
  fuera del alcance de esta fase.

## 18. Clasificación final

**PASS PARCIAL.** Las capacidades operativas, diagnóstico read-only,
administración segura, capacidad específica, ADM-04, ADM-16, OpenAI, Recovery,
leases, FIFO, Race Detector macOS, crash real y cleanup pasan. Linux no puede
clasificarse completo por la fixture de transición `STOPPING` bloqueada en esa
plataforma.

**Ruta:** `evidence/LARM-006.8.16-operational-readiness.md`
