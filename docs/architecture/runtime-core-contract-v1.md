# LARM Runtime Core Contract v1

**Estado:** contrato versionado para LARM-006.1; tipos y validadores puros implementados en `internal/corecontract/`.

## 1. Alcance

La versión de esquema es la cadena fija `"1"`. El contrato es independiente del harness y no conoce `session_id`, hooks, providers, launcher Python ni eventos de OpenCode. Los IDs primarios son UUIDv4 canónicos en minúsculas; los tiempos son UTC.

El contrato define identidades, tipos JSON, estados, invariantes, evidencia y validadores. LARM-006.3a añade persistencia diagnóstica metadata-only del Runtime Core, sin convertir el journal en scheduler, coordinator ni mecanismo de recuperación automática.

## 2. Entidades

### Execution

`Execution` contiene `execution_id`, `parent_execution_id` opcional, `initiator`, `lifetime` (`PERSISTENT` o `EPHEMERAL`), `model_id`, estado, IDs de leases e inferencias, timestamps, resultado de finalización y `recovery_state`.

Una execution puede contener cualquier cantidad de inferencias secuenciales. La execution efímera creada para un cliente OpenAI directo sigue siendo una execution contractual con `lifetime=EPHEMERAL` e iniciador `openai-compatible`; no se atribuye a un harness.

El resultado de la execution (`Completion`) es independiente del resultado semántico de cada `Inference`.

### Inference

Cada intento HTTP tiene su propio `request_id`. Un retry conserva `execution_id`, `lease_id` y `model_id`, pero recibe otro `request_id`.

La entidad separa:

- **Admisión:** `PENDING`, `ACCEPTED`, `REJECTED`.
- **Transporte:** `NOT_STARTED`, `STARTED`, `HEADERS_RECEIVED`, `BODY_COMPLETED`, `COMPLETED`, `FAILED`, `UNKNOWN`.
- **Semántica:** `NOT_EVALUATED`, `SUCCEEDED`, `FAILED`, `TRUNCATED`, `UNKNOWN`.
- **Cancelación:** `NONE`, `CANCEL_REQUESTED`, `CANCEL_IN_PROGRESS`, `CANCEL_CONFIRMED`, `CANCEL_UNKNOWN`.

La asignación de proceso es explícita: `PENDING` sin identidad, `UNVERIFIED` cuando el supervisor solo aporta metadatos no durables, o `ASSIGNED` con un ID válido. El transporte puede observarse con `UNVERIFIED`, pero nunca se fabrica un `process_instance_id`; la identidad durable sigue siendo responsabilidad del Process Supervisor futuro.

### Lease

`Lease` contiene `lease_id`, `execution_id`, `ownership` (`PERSISTENT_EXECUTION` o `EPHEMERAL_EXECUTION`), modelo, estado FIFO, estado de activación, inferencias admitidas, expiración, heartbeat, liberación y recovery.

Los estados FIFO son `QUEUED`, `PREPARING`, `ACTIVE`, `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED` y `UNKNOWN`. La expiración y el heartbeat no son evidencia de terminación.

### ProcessInstance

`ProcessInstance` contiene su propio `process_instance_id`, `owner_instance_id`/`larm_instance_id` de LARM, modelo, PID/PGID como metadata, ejecutable, `started_at`, identidad OS de inicio, fingerprint de configuración, readiness, ownership, estado de terminación, inferencias, evidencia de terminación y recovery. `StartIdentity` no es un timestamp de pared: es una identidad de nacimiento suministrada por macOS/Linux.

Los estados son `PREPARING`, `STARTING`, `WAITING_READINESS`, `READY`, `TERMINATION_REQUESTED`, `TERMINATING`, `TERMINATED_VERIFIED`, `TERMINATION_UNKNOWN`, `PROCESS_LOST`, `FAILED`, `IDENTITY_UNKNOWN` y `RECOVERY_REQUIRED`.

PID no es identidad durable. `cmd.Wait` solo observa al líder y no prueba la desaparición de descendientes. Un puerto esperado tampoco prueba ownership. `TERMINATED_VERIFIED` exige identidad verificada, líder recolectado y ausencia verificable del grupo y descendientes observados.

## 3. Clasificación de estados

| Entidad | Activos | Terminales | Ambiguos/protegidos | Recuperables explícitamente |
|---|---|---|---|---|
| Execution | `CREATED`, `WAITING_MODEL`, `ACTIVE`, `FINALIZING`, `CANCEL_REQUESTED` | `COMPLETED`, `CANCEL_CONFIRMED`, `FAILED` | `UNKNOWN`, `RECOVERY_REQUIRED` | `RECOVERY_REQUIRED` |
| Inference | admisión aceptada y transporte no terminal | transporte completado, o transporte fallido con semántica fallida/truncada y evidencia terminal, o cancelación confirmada | cualquier `UNKNOWN`, cancelación desconocida, SSE truncado conserva fallo no éxito | `UNKNOWN` mediante evidencia posterior |
| Lease | FIFO no terminal, release no confirmado | FIFO terminal y release confirmado | FIFO/release/recovery desconocidos | recovery operator-gated |
| ProcessInstance | preparación, inicio, readiness, ready, terminación | `TERMINATED_VERIFIED`, `FAILED` | `IDENTITY_UNKNOWN`, `RECOVERY_REQUIRED` | recovery explícito con identidad verificable |

Un estado ambiguo conserva ownership y recursos. Ningún timeout, EOF downstream, heartbeat ausente o retorno del agente libera por sí solo.

## 4. Tablas de transición

### Execution

| Desde | Hacia |
|---|---|
| `CREATED` | `WAITING_MODEL`, `CANCEL_REQUESTED`, `FAILED`, `RECOVERY_REQUIRED` |
| `WAITING_MODEL` | `ACTIVE`, `FINALIZING`, `CANCEL_REQUESTED`, `FAILED`, `UNKNOWN`, `RECOVERY_REQUIRED` |
| `ACTIVE` | `WAITING_MODEL`, `FINALIZING`, `CANCEL_REQUESTED`, `FAILED`, `UNKNOWN`, `RECOVERY_REQUIRED` |
| `FINALIZING` | `COMPLETED`, `CANCEL_CONFIRMED`, `FAILED`, `UNKNOWN`, `RECOVERY_REQUIRED` |
| `CANCEL_REQUESTED` | `CANCEL_CONFIRMED`, `FAILED`, `UNKNOWN`, `RECOVERY_REQUIRED` |
| `UNKNOWN` | `RECOVERY_REQUIRED` |
| `RECOVERY_REQUIRED` | `COMPLETED`, `CANCEL_CONFIRMED`, `FAILED`, con evidencia explícita |

Una execution terminal no puede reactivarse. `FINALIZING` mantiene ownership hasta verificar todas sus inferencias.

### Inference

Las dimensiones avanzan independientemente, pero sus invariantes se validan juntas:

```text
admission:   PENDING → ACCEPTED | REJECTED
transport:   NOT_STARTED → STARTED → HEADERS_RECEIVED
             HEADERS_RECEIVED → BODY_COMPLETED | COMPLETED | FAILED | UNKNOWN
             BODY_COMPLETED → COMPLETED | FAILED | UNKNOWN
semantic:    NOT_EVALUATED → SUCCEEDED | FAILED | TRUNCATED | UNKNOWN
cancel:      NONE → CANCEL_REQUESTED → CANCEL_IN_PROGRESS
             CANCEL_REQUESTED → CANCEL_CONFIRMED | CANCEL_UNKNOWN
             CANCEL_IN_PROGRESS → CANCEL_CONFIRMED | CANCEL_UNKNOWN
```

Los eventos repetidos son idempotentes cuando no cambian el estado. Los eventos tardíos que intentan modificar una inferencia terminal se rechazan.

### Lease

```text
FIFO:     QUEUED → PREPARING → ACTIVE → COMPLETED|FAILED|CANCELLED|EXPIRED|UNKNOWN
release:  NOT_REQUESTED → REQUESTED → IN_PROGRESS → RELEASED
          cualquier estado no confirmado → UNKNOWN
activate: INACTIVE → PREPARING → ACTIVE → RELEASING → INACTIVE
```

Una lease liberada no se reactiva. No se permite liberar si alguna inferencia asociada es activa o ambigua.

### ProcessInstance

```text
PREPARING → STARTING → WAITING_READINESS → READY
READY → TERMINATION_REQUESTED → TERMINATING → TERMINATED_VERIFIED
                       ↘ FAILED / IDENTITY_UNKNOWN / RECOVERY_REQUIRED
IDENTITY_UNKNOWN → RECOVERY_REQUIRED
RECOVERY_REQUIRED → TERMINATED_VERIFIED | FAILED, con evidencia
```

`TERMINATED_VERIFIED` requiere evidencia de identidad verificada y ausencia del proceso. La existencia de PID, PGID o `cmd.Wait` sin esa evidencia no basta.

## 5. Invariantes de ownership

- Toda inferencia admitida tiene `request_id`, `execution_id`, `lease_id` y `model_id` válidos.
- El proceso puede estar pendiente al admitir; el transporte no puede empezar pendiente.
- `ValidateInferenceOwnership` exige que execution, lease, inferencia y modelo coincidan.
- Una lease no cambia de propietario, ejecución, modelo o clase de ownership.
- Una ejecución cancelada o terminal no admite nuevas inferencias.
- Una lease liberada no puede ser reactivada por eventos tardíos.
- `UNKNOWN`, `CANCEL_UNKNOWN`, `TransportUnknown` y `SemanticUnknown` mantienen protección.
- `ValidateSingleModelResidence` permite como máximo una residencia FIFO activa, preservando la política actual de un modelo residente y sin crear scheduler.

## 6. SSE y transporte

`SSEObservation` representa explícitamente headers, primer chunk, chunks intermedios, detección de `[DONE]`, EOF, error de lectura, desconexión downstream, error backend y truncamiento. `SSEEventType` enumera esas observaciones.

El contrato no implementa el parser SSE. La capa observadora debe detectar `[DONE]` incluso si está fragmentado entre lecturas y solo publicar `done_detected=true` cuando el marcador completo esté identificado.

Para chat completions streaming:

- headers no son terminación;
- primer chunk y chunks intermedios no son terminación;
- `[DONE]` sin EOF aún no es éxito completo;
- EOF sin `[DONE]` es `TRUNCATED`/fallo, nunca éxito;
- error de lectura, backend o desconexión no se convierten automáticamente en cancelación;
- el simple cierre downstream no confirma terminación upstream.

Para JSON, `HTTP_COMPLETED` con EOF puede cerrar la semántica de transporte. El éxito semántico requiere evidencia correlacionada y no prueba terminación del proceso.

## 7. Cancelación

`CANCEL_REQUESTED` expresa intención. `CANCEL_IN_PROGRESS` expresa propagación. `CANCEL_CONFIRMED` exige evidencia correlacionada del transporte administrado terminado o de la terminación verificada del backend. Si esa prueba se pierde, se conserva `CANCEL_UNKNOWN` y ownership protegido.

El contrato no envía señales ni cancela procesos. Tampoco usa timeout como evidencia.

## 8. Evidencia terminal

`TerminalEvidence` contiene únicamente metadata: ID del evento, sujeto (request, process, lease o execution), source, tipo, timestamp, resultado, correlación, alcance de garantía y flags de protocolo/identidad.

Tipos mínimos implementados:

`HTTP_COMPLETED`, `SSE_COMPLETED`, `SSE_TRUNCATED`, `TRANSPORT_ERROR`, `BACKEND_ERROR`, `CANCELLATION_CONFIRMED`, `PROCESS_TERMINATION_VERIFIED`, `EXECUTION_COMPLETED` y `UNKNOWN`.

Una evidencia de transporte no se convierte en evidencia de proceso. Una evidencia de proceso no se convierte en éxito semántico. No hay campos para prompts, respuestas, tokens o credenciales.

## 9. Compatibilidad

| Estado/capacidad actual | Estado contractual | Compatibilidad | Riesgo |
|---|---|---|---|
| `executiongate.CREATED` | `Execution.CREATED` | Directa | El gate sigue siendo segunda autoridad hasta una fase posterior. |
| `executiongate.ACTIVE` | `Execution.ACTIVE` | Directa | El ownership actual es por sesión; no se importa esa identidad. |
| `executiongate.FINALIZING` | `Execution.FINALIZING` | Directa | Requiere conservar inferencias activas/ambiguas. |
| `executiongate.COMPLETED/CANCELLED/FAILED` | `COMPLETED/CANCEL_CONFIRMED/FAILED` | Mapeo explícito | `CANCELLED` contractual exige confirmación, no solo intención. |
| `executiongate.UNKNOWN` | `UNKNOWN` | Se conserva | No se libera ni se reintenta silenciosamente. |
| `scheduler.QUEUED/PREPARING/ACTIVE` | `Lease` FIFO equivalente | Directa | El scheduler existente no conoce execution ownership. |
| `scheduler.COMPLETED/FAILED/CANCELLED/EXPIRED` | Lease FIFO terminal equivalente | Directa | TTL no es evidencia de inferencia terminal. |
| `scheduler.RECOVERY_REQUIRED` | `Lease.recovery_state=RECOVERY_REQUIRED` | Se conserva | Es fail-closed, no recuperación automática. |
| `runtime.STARTING/READY/STOPPING` | `ProcessInstance.STARTING/READY/TERMINATING` | Integrada en LARM-006.4 | `TERMINATION_UNKNOWN` conserva el bloqueo cuando el árbol no es verificable. |
| `runtime.FAILED` | `ProcessInstance.FAILED` | Compatible | No prueba por sí solo el estado de cada inferencia. |
| Proxy OpenAI sin contexto | Execution efímera + lease correlacionada | Integrada en LARM-006.2a | La execution generada es interna y no persistente. |
| SSE actual que acepta EOF | `SSE_TRUNCATED` sin `[DONE]` | Incompatible intencional | Integrarlo en LARM-006.2; no tocar proxy en esta fase. |

No se sustituyen ni se eliminan tipos de producción. `ProcessAssignment=UNVERIFIED` se conserva para backends sin identidad, pero el supervisor administrado usa `ProcessAssignment=ASSIGNED` solo tras persistir una identidad durable verificable. `UNKNOWN`, `FINALIZING` y `RECOVERY_REQUIRED` permanecen representables y protegidos.

## 10. Persistencia y recuperación diagnóstica

Desde LARM-006.3a, el Runtime Core puede persistir snapshots metadata-only de inferencias y su correlación con execution, lease y modelo en un journal versionado. La secuencia crítica es: validar transición, sincronizar el journal de forma durable, publicar en memoria y solo después permitir el transporte protegido. El journal usa escritura temporal, `fsync` del archivo, `rename` atómico y sincronización del directorio en macOS/Linux.

Un journal no cerrado limpiamente, con una inferencia no terminal, `UNKNOWN`, lease protegida o corrupción no se interpreta como éxito. El arranque conserva la evidencia disponible, marca `RECOVERY_REQUIRED`, bloquea nuevas admisiones y no adopta, termina ni reasigna procesos o leases. La ausencia de `process_instance_id` continúa representándose como `UNVERIFIED`; PID y puerto no son identidad durable.

El diagnóstico de solo lectura está disponible en `GET /api/v1/recovery/status`, `/api/v1/recovery/inferences` y `/api/v1/recovery/leases`. No existe desbloqueo HTTP ni recuperación automática.

LARM-006.3b añade un servicio de incidentes operator-gated, sin segunda autoridad de release. Correlaciona el journal de inferencias con el store de idempotencia durable del scheduler, agrupa condiciones por una clave determinista y conserva `RecoveryIncident` y `RecoveryPlan` en un sidecar metadata-only (`<journal>.operator.json`). Los planes incluyen digest de evidencia, versión de snapshot, precondiciones y clasificación fail-closed. La evidencia registrada no se eleva a evidencia verificada por una autorización humana.

La API expone únicamente consultas: `GET /api/v1/recovery/incidents`, `GET /api/v1/recovery/incidents/{incident_id}`, `GET /api/v1/recovery/incidents/{incident_id}/plan` y `GET /api/v1/recovery/audit`. No se añadió un endpoint de ejecución. La implementación conserva la operación interna de confirmación como rechazo auditable porque el servidor loopback no dispone de autenticación administrativa verificable; ningún `operator_id` suministrado por el cliente puede ser autenticación.

### 10.1 Integración LARM-006.4

El Process Supervisor reutiliza este journal; no crea un daemon ni un journal paralelo. Cada spawn escribe una intención `PREPARING` antes de `exec`, luego persiste `STARTING` con PID, PGID, ejecutable y `os_process_start_identity`, y solo después de persistir `READY` y la evidencia de health publica el estado operativo. Un fallo de persistencia después de `exec` deja el runtime bloqueado y conserva la política fail-closed.

En macOS la identidad se obtiene mediante `proc_pidinfo(PROC_PIDTBSDINFO)` de `libproc`, usando `pbi_start_tvsec`, `pbi_start_tvusec` y PID. En Linux se parsea `/proc/<pid>/stat` desde el delimitador final de `comm` (no desde un `Fields` ingenuo), usa el campo `starttime` y lo combina con `/proc/sys/kernel/random/boot_id`. PID o un timestamp de pared aislado nunca autorizan una señal.

`OWNED_VERIFIED` solo existe para el proceso creado por el supervisor cuya identidad de nacimiento, grupo y evidencia de spawn coinciden. Tras reiniciar, una identidad sobreviviente coincidente es diagnóstico, no adopción: se clasifica `UNKNOWN` y queda `RECOVERY_REQUIRED`; una identidad diferente se clasifica `FOREIGN`. Ninguno recibe señales.

La terminación valida identidad y PGID antes de SIGTERM. Solo escala a SIGKILL mientras el líder y su identidad siguen verificables. Después de `Wait`, se inspeccionan miembros del grupo y descendientes observados; si queda un proceso, la identidad cambió o la enumeración no es fiable, el resultado es `TERMINATION_UNKNOWN`. Solo la evidencia durable `PROCESS_TERMINATION_VERIFIED` puede producir `TERMINATED_VERIFIED`.

El Registry asigna `process_instance_id` y estado de identidad durable antes de `http.Client.Do`. Las inferencias no comienzan en `STOP_REQUESTED`/`TERMINATING`/`TERMINATION_UNKNOWN`/`RECOVERY_REQUIRED`; una asignación pendiente o no verificable no se convierte en una identidad inventada. Los endpoints de procesos son solo lectura: `GET /api/v1/processes` y `GET /api/v1/processes/{process_instance_id}`.

## 11. Fuera de alcance

No forman parte de este contrato la recuperación automática, la adopción o terminación post-reinicio de procesos, la liberación administrativa de `UNKNOWN`, la migración de `executiongate`, cambios a OpenCode, un segundo scheduler/coordinador, ni ejecución administrativa de Recovery Plans. Las resoluciones siguen bloqueadas hasta que exista identidad administrativa y evidencia compatibles.
