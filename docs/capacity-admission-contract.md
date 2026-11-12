# LARM-007.3.2a — Contrato de admisión basada en capacidad

**Estado:** contrato endurecido para una implementación futura. Este documento
no implementa enforcement, no cambia decisiones del Scheduler y no añade claves
al parser operativo.

## 1. Decisión resumida

La capacidad debe evaluarse fuera del Scheduler mediante una interfaz
read-only. El resultado es una observación fechada, no una reserva de memoria ni
una garantía de que `llama-server` podrá iniciar. Si en el futuro se habilita
enforcement, la decisión debe ocurrir únicamente para el elemento que ya está
en la cabeza de la cola FIFO, después de comprobar Recovery y justo antes de
la activación. Una reevaluación debe preceder a cada intento real de carga.

La política propuesta es:

| Veredicto | Comportamiento futuro | Comportamiento actual |
|---|---|---|
| `FIT` | Permite intentar continuar, sujeto a una comprobación final de estado. | Se observa en Planner/Shadow; nunca autoriza ni cambia Scheduler. |
| `INSUFFICIENT` | No inicia una carga nueva. Espera con límite y reevalúa, o cancela/rechaza al vencer el límite. | Observación no bloqueante; Scheduler continúa igual. |
| `UNKNOWN` | Por defecto `allow`, para conservar compatibilidad; nunca se transforma en `FIT` y se registra el riesgo. | Observación no bloqueante; Scheduler continúa igual. |

`RESIDENT_REUSE` no es una nueva carga: no vuelve a contabilizar el modelo
completo, pero exige probar que el proceso residente sigue siendo el proceso
administrado, está `READY`, pertenece al modelo solicitado y no está protegido
por Recovery.

Decisiones cerradas en esta revisión:

- `WAITING_CAPACITY` es una fase interna de política. No es un nuevo
  `LeaseState`: mientras espera, la lease continúa `QUEUED` y conserva su lugar
  lógico en FIFO.
- La solicitud en espera bloquea la cabeza lógica. No se hace `skip` de la
  cabeza ni se activa una solicitud posterior.
- La espera no publica `PREPARING`, no inicia procesos, no reserva memoria, no
  mantiene locks durante una evaluación y no extiende TTL ni heartbeat.
- El límite efectivo es el deadline más próximo entre admisión, cola del proxy
  y contexto de la solicitud. La cancelación, Recovery y shutdown interrumpen
  la espera inmediatamente.
- Los resultados de capacidad son observaciones; solo una evaluación vigente y
  una comprobación final serializada pueden permitir intentar la activación.
- `admission.enabled=false` conserva el comportamiento observable actual,
  incluidas las respuestas HTTP y SSE.

## 2. Contratos existentes inspeccionados

| Área | Contrato real relevante | Consecuencia para admisión |
|---|---|---|
| Scheduler | `RequestWithRequestID` y `RequestIdempotent` crean una lease `QUEUED`, la insertan al final de la cola y notifican un observer sin esperar su resultado. | Son los puntos de correlación, no una decisión de capacidad. FIFO, idempotencia y ownership siguen siendo del Scheduler. |
| FIFO/worker | `step` hace `pop`, persiste `PREPARING`, llama `activate` y solo después `finishActivation` publica `ACTIVE`. | Una evaluación no puede adelantar una solicitud ni convertirla en `ACTIVE`. |
| Lease | TTL y `max_lease_duration` se calculan al pasar a `ACTIVE`; heartbeat solo aplica a una lease activa. | No se puede suponer que una lease activa pueda conservarse durante una espera de capacidad prolongada. |
| Runtime Manager | `CurrentProcessForModel` verifica estado `READY`, instancia y la identidad OS del proceso; `Start` persiste la intención antes de hacer `exec`. | La identidad residente y la carga siguen siendo autoridad de Runtime. |
| Planner | `Planner.Plan` consume una copia de `memory.Snapshot` y metadata GGUF; devuelve `FIT`, `INSUFFICIENT` o `UNKNOWN`. | El resultado no tiene efecto operativo y puede quedar obsoleto inmediatamente. |
| Memory Observer | El snapshot pasa a `STALE` después de más de tres intervalos; bytes nulos significan desconocido. | `STALE`, `UNAVAILABLE`, `PARTIAL` o proveedor GPU incompleto no deben ser `FIT`. |
| Shadow | `ObserveAdmission` encola de forma asíncrona; errores, timeout y cola llena quedan como observaciones. | El modo shadow debe permanecer idéntico y nunca esperar al Planner. |
| Recovery | `AdmissionGate`, `manager.RecoveryRequired` y el registry bloquean nuevas admisiones/arranques; un reinicio no adopta procesos. | Recovery tiene precedencia sobre cualquier resultado de capacidad. |
| Execution Gate | Conserva ownership, solicitudes activas/ambiguas y estados `UNKNOWN`/`RECOVERY_REQUIRED`; no libera por timeout. | La capacidad no puede saltarse el gate ni liberar una lease protegida. |

Referencias de implementación: `internal/scheduler/{scheduler.go,queue.go,state.go}`,
`internal/runtime/manager.go`, `internal/capacity/{planner.go,shadow.go}`,
`internal/memory/{observer.go,types.go}`, `internal/inference/recovery.go`,
`internal/executiongate/{coordinator.go,types.go}` y
`internal/recovery/service.go`.

### 2.1 Puntos candidatos sin alterar garantías

1. **Captura de contexto:** al crear la solicitud, el Scheduler puede entregar
   `request_id`, `lease_id`, modelo y operación observada a un componente
   desacoplado. Es el equivalente futuro del callback `AdmissionObserver`.
2. **Gate de Recovery:** antes de cualquier política nueva se debe comprobar el
   `AdmissionGate` existente y `manager.RecoveryRequired`.
3. **Cabeza FIFO:** después de `queue.pop`, o mediante una vista lógica que no
   elimine el elemento todavía, el Scheduler puede solicitar una evaluación
   síncrona/asíncrona con timeout acotado. No se debe evaluar y activar en
   paralelo fuera del worker único.
4. **Última comprobación:** justo antes de `manager.Start`, comprobar de nuevo
   lease no terminal, cancelación, identidad residente y Recovery. `FIT` no
   sustituye esa comprobación.
5. **Resultado de carga:** `finishActivation` conserva la autoridad actual para
   publicar `ACTIVE` o `FAILED`; un fallo de capacidad de Runtime sigue siendo
   un fallo de Runtime, no se convierte retrospectivamente en `FIT`.

No se recomienda introducir una segunda cola, un contador de memoria reservado
ni mutaciones desde `internal/capacity`.

### 2.2 Funciones concretas del Scheduler para una integración futura

Estas son las funciones que tendrían que cambiar en una implementación futura;
la lista no autoriza modificarlas en esta tarea:

| Función | Cambio contractual futuro |
|---|---|
| `(*Scheduler).Request` / `RequestWithRequestID` | Propagar el contexto/deadline y la correlación de admisión a la solicitud futura sin cambiar la creación de la lease `QUEUED` ni el orden de `queue.push`. `Request` solo hereda este comportamiento. |
| `(*Scheduler).RequestIdempotent` | Asociar la espera al registro idempotente y conservar replay, fingerprint, orden y Recovery; un replay no crea una segunda evaluación ni reinicia el deadline original. |
| `(*Scheduler).admissionOperationLocked` | Dejar de usar `QUEUED` como operación de capacidad: la posición es un dato de Scheduler; la evaluación debe clasificar `NEW_LOAD` o `RESIDENT_REUSE` con evidencia viva. |
| `leaseQueue.pop` y `(*Scheduler).step` | Sustituir el `pop` destructivo previo a `PREPARING` por una vista/commit de cabeza: evaluar fuera de `s.mu`, conservar la lease y la posición lógica mientras sea `WAITING_CAPACITY`, y solo retirar/avanzar tras `FIT` y la comprobación final. |
| `(*Scheduler).WaitActive` | Observar la terminación de una espera de admisión y conservar la distinción entre cancelación, deadline de cola, timeout de capacidad, Recovery y cierre; no convertir la espera interna en `PREPARING`. |
| `(*Scheduler).release` / `Release` / `ReleaseAutomatic` | Hacer que la cancelación idempotente despierte y retire una espera `QUEUED`, sin liberar una lease protegida ni depender de heartbeat. |
| `(*Scheduler).Shutdown` / `cleanup` | Cancelar explícitamente el contexto de admisión, cerrar las esperas y aplicar el cleanup actual sin dejar evaluaciones ni leases ficticias. |
| `(*Scheduler).activate` | Ejecutar únicamente después del `FIT` vigente; repetir la comprobación de residente, Recovery, lease, cancelación y deadline antes de `manager.Start`. |
| `(*Scheduler).finishActivation` | Descartar resultados tardíos o activaciones canceladas y conservar los estados/errores existentes; una evaluación tardía nunca revive ni autoriza una lease. |
| `(*Scheduler).worker` / `notify` / `signalChanged` | Despertar por medición, cancelación, Recovery, timeout y shutdown sin crear un worker por solicitud ni evaluar bajo `s.mu`. |

`Get` y `statusLocked` deben seguir exponiendo `QUEUED` y la posición FIFO; no
deben publicar `WAITING_CAPACITY` como `LeaseState`. `worker` y `notify` podrán
requerir ajustes de integración para despertar por medición o cancelación, pero
no deben evaluar bajo `s.mu`. `internal/capacity`
no puede llamar a estas funciones ni mutar Scheduler, Runtime, leases o
Recovery.

Las firmas actuales de `Request*` no reciben `context.Context`; la integración
futura tendrá que propagar el contexto desde cada handler/adaptador antes de
calcular `D_effective` o introducir una capa explícita de correlación. Eso no
autoriza cambiar ahora el contrato del proxy ni implementar enforcement.

## 3. Interfaz de evaluación propuesta

La interfaz es conceptual y no debe añadirse al paquete operativo en esta
tarea. Debe depender de tipos de contrato y no de `Scheduler` concreto:

```go
type AdmissionEvaluator interface {
    Evaluate(context.Context, AdmissionInput) (AdmissionEvaluation, error)
}

type AdmissionInput struct {
    ModelID          string
    RequestID        string
    LeaseID          string // opcional para una observación sin lease externa
    Operation        AdmissionOperation
    ObservedResident *ResidentIdentity
    EvaluationReason string // initial, retry, recheck, stale, etc.
}

type AdmissionEvaluation struct {
    EvaluationID string
    ModelID      string
    RequestID    string
    LeaseID      string
    Operation    AdmissionOperation
    Verdict      Verdict
    Snapshot     SnapshotEvidence
    Memory       MemoryEvidence
    Source       []string
    Justification []string
    Resident     *ResidentIdentity
    Status       EvaluationStatus // completed, error, timeout, cancelled, discarded
    Cause        string           // causa sanitizada, si aplica
    EvaluatedAt  time.Time
}
```

Los nombres son ilustrativos; el contrato semántico es obligatorio. Los enums
de operación son `NEW_LOAD` y `RESIDENT_REUSE`; `QUEUED` es un estado/posición
de Scheduler y no una operación. Los veredictos son `FIT`, `INSUFFICIENT` y
`UNKNOWN`.

### 3.1 Evidencia mínima

```json
{
  "evaluation_id": "uuid",
  "model_id": "code-fast",
  "request_id": "uuid",
  "lease_id": "uuid",
  "operation": "NEW_LOAD",
  "verdict": "FIT",
  "snapshot": {
    "state": "AVAILABLE",
    "measured_at": "2026-11-12T10:00:00Z",
    "age": "1.2s",
    "source": "larm-memory-observer"
  },
  "memory": {
    "required_bytes": 12000000000,
    "available_bytes": 17179869184,
    "system_required_bytes": 12000000000,
    "system_available_bytes": 17179869184,
    "gpu_required_bytes": 0,
    "gpu_available_bytes": null
  },
  "source": ["larm-memory-observer", "gguf-metadata"],
  "justification": ["la demanda estimada cabe en la memoria observada"],
  "evaluated_at": "2026-11-12T10:00:01Z"
}
```

Requisitos:

- `model_id` y `request_id` son obligatorios. `lease_id` es obligatorio cuando
  la solicitud ya tiene lease; no se inventa para una mera observación.
- `operation` describe lo que se observó, pero no es una orden. Si al momento
  de ejecutar el residente ya no coincide, se reclasifica y se reevalúa.
- `snapshot.state` conserva al menos `AVAILABLE`, `STALE`, `PARTIAL`,
  `UNAVAILABLE` y `PERMISSION_DENIED`; `age`, `measured_at` y `source` son
  obligatorios aunque la medición sea inválida.
- Los bytes desconocidos son `null`, nunca cero. Deben distinguirse memoria del
  sistema, VRAM y memoria requerida por dominio. `required_bytes` no significa
  consumo garantizado.
- `source` y `justification` explican Planner, Observer, metadata GGUF y
  limitaciones. No incluyen prompts, respuestas, rutas absolutas ni secretos.
- `evaluated_at` es el timestamp de la evaluación; no es un vencimiento ni un
  lease token.
- `ResidentIdentity` incluye, cuando aplica, `model_id`, `pid`,
  `process_instance_id`, `start_identity`, `runtime_state` y
  `ownership_status`. PID solo nunca es identidad suficiente.

### 3.2 Idempotencia y reevaluación

- Cada evaluación tiene `evaluation_id` único. Repetir la publicación del mismo
  ID es idempotente y no produce otra decisión observable.
- La clave lógica de una solicitud es `(request_id, lease_id, model_id)`; el
  primer resultado puede correlacionarse con ese intento, pero no debe
  reutilizarse después de una espera.
- Cada reevaluación genera un nuevo `evaluation_id`, `evaluated_at`, snapshot y
  justificación, con `EvaluationReason=retry|recheck|stale|timeout-retry`.
  Nunca se conserva `FIT` solo porque una evaluación anterior fue `FIT`.
- Evaluar no reserva memoria, no cambia FIFO, no renueva heartbeat, no extiende
  TTL y no crea persistencia nueva. Si el mismo request llega dos veces, la
  idempotencia de adquisición existente sigue siendo la autoridad.
- La evaluación puede ser calculada fuera del lock del Scheduler, pero la
  decisión de continuar debe validarse dentro de la transición que precede a
  `manager.Start`. Si cambió lease, Recovery, residencia o cancelación, se
  descarta el resultado.

### 3.3 Contrato de `AdmissionEvaluator` y concurrencia

El evaluador conceptual debe cumplir todo lo siguiente:

- `Evaluate(ctx, input)` respeta `context.Context`; cada evaluación tiene un
  timeout finito propio, menor o igual al presupuesto restante de la espera.
- La llamada se ejecuta sin locks del Scheduler. El Scheduler toma un snapshot
  inmutable, libera el lock, evalúa y vuelve a validar la generación de la
  solicitud bajo su serialización.
- La concurrencia está limitada por una capacidad fija/configurable y una cola
  acotada. Si el límite está lleno, la solicitud espera mediante el mismo
  contexto o recibe `UNKNOWN` por evaluación no disponible; nunca se crean
  goroutines ilimitadas ni una goroutine por tick sin control.
- El timeout no autoriza a seguir creando goroutines que el proveedor no puede
  cancelar: una llamada no cooperativa puede retener un slot acotado hasta
  terminar, mientras las nuevas solicitudes reciben `UNKNOWN`/no disponible o
  esperan con su propio deadline. El pool no crece para compensarla.
- Un panic, error o timeout del evaluador se recupera en el límite del
  evaluador, se registra como `UNKNOWN` y conserva la causa sanitizada en la
  evidencia. No derriba el worker.
- El evaluador es read-only: no modifica leases, Scheduler, Runtime ni
  Recovery.
- Cada resultado lleva `evaluation_id` y una generación/clave lógica. Si el
  contexto fue cancelado, la lease se volvió terminal o ya empezó otra
  reevaluación, el resultado tardío se registra como descartado y no puede
  autorizar `PREPARING`, `Start` ni `ACTIVE`.
- Un `FIT` que termina después de una cancelación, timeout, shutdown o cambio
  de residencia nunca se reaplica. La activación debe pedir un nuevo `FIT` si
  la operación todavía es válida.

## 4. Política futura de admisión

### `FIT`

`FIT` permite intentar la ruta normal. No garantiza disponibilidad posterior:
el snapshot puede envejecer, un proceso externo puede consumir memoria y el
allocator de llama.cpp puede requerir más bytes. La comprobación final debe
seguir verificando Recovery, lease, FIFO y Runtime.

### `INSUFFICIENT`

- No se inicia una nueva carga para el elemento evaluado.
- Se conserva el orden FIFO: el elemento de cabeza espera o termina; no se
  salta para ejecutar una solicitud posterior.
- La espera recomendada es una espera lógica sobre una lease todavía
  `QUEUED`, con reevaluación periódica y un `wait_timeout` finito. No se publica
  `PREPARING`/`ACTIVE` mientras se espera capacidad.
- La espera debe despertar por: nueva medición, cambio de residencia,
  liberación/cambio de Runtime, cancelación, Recovery, timeout o shutdown.
- Al timeout se cancela/termina la adquisición sin iniciar proceso y se devuelve
  un error de capacidad explícito. La cancelación del cliente debe retirar la
  entrada usando la operación existente; no se usa heartbeat ficticio.
- No se mantienen indefinidamente leases activas, locks de Runtime, procesos,
  reservas de memoria ni tareas de planner. Una lease `QUEUED` tampoco puede
  depender de un heartbeat de lease `ACTIVE`.

Un producto futuro podría ofrecer `reject` en lugar de `wait`; debe ser una
decisión explícita y conservar el estado terminal normal de la lease. No se
propone “skip” porque rompería FIFO.

### `UNKNOWN`

El valor por defecto futuro es `allow`, compatible con el comportamiento actual:
continúa por la ruta normal y registra que no hubo medición confiable. `UNKNOWN`
no se codifica ni se presenta como `FIT`. Un modo más restrictivo (`wait` o
`reject`) puede aprobarse después, pero debe distinguir timeout del Planner,
snapshot `STALE`, ausencia de VRAM, permisos y fallo de metadata.

### `RESIDENT_REUSE`

El Planner no debe volver a cobrar el archivo, KV-cache y workspace completos
si el mismo proceso está residente. La evaluación puede expresar carga de
modelo nueva igual a cero y, si el futuro modelo de memoria lo soporta, solo
memoria incremental de la inferencia; no debe prometer que esa memoria es cero.

Antes de reutilizar se exige:

1. `manager.CurrentProcessForModel(model_id)` confirma `READY` e identidad OS.
2. `process_instance_id`, modelo, ownership y estado coinciden con la evidencia
   usada por la evaluación.
3. Recovery, Execution Gate y lease permiten la solicitud.
4. La comprobación final ocurre bajo la serialización del Scheduler.

Si cualquiera falla, la evaluación anterior se descarta y la operación se
convierte en `NEW_LOAD` o `UNKNOWN` según la evidencia, nunca en una
reutilización silenciosa. No se realiza una estimación completa de carga nueva
mientras la identidad, instancia, estado `READY`, modelo, ownership, Recovery,
Execution Gate y lease sigan siendo válidos. Esto no implica memoria
incremental cero: la memoria de inferencia/KV-cache/workspace incremental debe
modelarse cuando exista evidencia; los bytes faltantes permanecen desconocidos.
La residencia no comparte ownership de lease, no evita Recovery y no libera
restricciones de leases.

## 5. Leases, FIFO y máquina de estados conceptual

La siguiente máquina es una vista de política, no una ampliación de
`LeaseState` en esta tarea:

```text
solicitud
   │
   ├─ admission.enabled=false ───────────────► flujo actual
   │
   ▼
QUEUED / cabeza FIFO
   │  Recovery/lease/cancelación válidos
   ▼
EVALUATING ── UNKNOWN + allow ───────────────► FINAL_CHECK
   │
   ├─ FIT ────────────────────────────────────► FINAL_CHECK
   ├─ INSUFFICIENT + wait ─► WAITING_CAPACITY ─┐
   │                                           │ reevalúa
   ├─ UNKNOWN + reject ───────────────────────► CANCELLED
   └─ error/timeout ──────────────────────────► UNKNOWN policy
                                               │
 WAITING_CAPACITY ─ timeout/cancel ────────────► CANCELLED + typed error
 WAITING_CAPACITY ─ recovery ──────────────────► blocked by Recovery
                                               │
FINAL_CHECK ─ resident válido ───────────────► PREPARING/ACTIVE
FINAL_CHECK ─ nueva carga ───────────────────► PREPARING → ACTIVE
PREPARING/ACTIVE ─ error ─────────────────────► FAILED
ACTIVE ─ release/expiry normal ───────────────► COMPLETED/EXPIRED
```

Reglas de ownership:

- `QUEUED` conserva la posición FIFO mientras espera; cancelación, timeout,
  Recovery o shutdown realizan la transición terminal y retiran la entrada por
  la autoridad existente.
- `WAITING_CAPACITY` no equivale a `PREPARING` y no adquiere recursos.
- La cancelación explícita o la expiración de `D_effective` usa la ruta de
  cleanup existente para una lease `QUEUED` y deja `CANCELLED`; el caller
  distingue `ErrAdmissionCancelled` de `ErrAdmissionTimeout`. `FAILED` queda
  para un fallo operativo real de activación/evaluación según la autoridad
  existente, no para inventar un nuevo estado de espera.
- Recovery interrumpe la espera y conserva la respuesta/bloqueo
  `RECOVERY_REQUIRED` de su autoridad; no se agrega un estado de lease para
  representarlo.
- La lease solo inicia TTL al `ACTIVE` actual. Si el diseño futuro necesita un
  límite de espera, debe ser un timeout de adquisición/cola, no una extensión
  del TTL activo.
- Una lease expirada o terminal no puede reactivarse por una evaluación tardía.
- Una solicitud cancelada gana una carrera contra un resultado `FIT` si la
  cancelación fue confirmada antes de la transición final.
- Recovery bloqueado conserva el bloqueo aunque el Planner diga `FIT`.

### 5.1 Transición de espera en la cabeza lógica

La transición futura debe ser una operación de política alrededor de la cabeza,
no una transición adicional de `LeaseState`:

1. El Scheduler observa el primer ID de `queue` sin retirarlo y toma, bajo
   `s.mu`, una copia inmutable de lease, generación, deadline y estado de
   Recovery.
2. Libera `s.mu` y ejecuta `AdmissionEvaluator` con un contexto hijo del
   `D_effective`. Durante este paso la lease sigue `QUEUED`, conserva
   `queue_position=1` y no se publica `PREPARING`.
3. Si el veredicto es `INSUFFICIENT` con `on_insufficient=wait`, registra la
   fase interna `WAITING_CAPACITY` asociada a esa generación. El worker no
   considera elegible ningún ID posterior; una reevaluación o un evento de
   cancelación vuelve a despertar la misma cabeza.
4. Para `FIT` (o `UNKNOWN` con `on_unknown=allow`), vuelve a tomar `s.mu` y
   comprueba generación, lease `QUEUED`, cancelación, deadline, Recovery y la
   identidad residente. Solo entonces hace el commit de cabeza que permite
   `PREPARING` y llama a `activate` fuera del lock.
5. Para timeout, cancelación o shutdown, realiza la transición terminal
   mediante la autoridad existente, normalmente `CANCELLED` para una lease
   `QUEUED`, conserva el error tipado de la causa, retira la entrada y
   despierta la siguiente cabeza. Recovery interrumpe y deja la operación
   bloqueada por su autoridad. Una espera cancelada no se convierte en
   `PREPARING`.

La evaluación no puede ejecutar `pop`, `Start`, `Release`, cambiar leases ni
llamar a Recovery. La única transición que puede autorizar activación es el
commit serializado posterior a la evaluación; por eso una solicitud posterior
no puede adelantarse aunque su evaluación resulte `FIT`.

## 6. Errores tipados, timeouts y reinicio

### 6.1 Contrato interno de errores

El proyecto usa sentinels con `errors.Is` y errores envueltos con contexto. La
admisión debe seguir ese patrón, sin depender de comparar mensajes:

```go
var (
    ErrAdmissionCapacity    = errors.New("admission capacity insufficient")
    ErrAdmissionTimeout     = errors.New("admission wait timed out")
    ErrAdmissionUnavailable = errors.New("admission evaluation unavailable")
    ErrAdmissionCancelled   = errors.New("admission cancelled")
    ErrAdmissionRecovery    = errors.New("admission requires recovery")
)

type AdmissionErrorKind string

type AdmissionError struct {
    Kind         AdmissionErrorKind
    RequestID    string
    LeaseID      string
    ModelID      string
    Operation    AdmissionOperation
    EvaluationID string
    Cause        error
    Retryable    bool
}

func (e *AdmissionError) Unwrap() error { return e.Cause }
```

`AdmissionErrorKind` es un enum conceptual con los valores `capacity`,
`timeout`, `unavailable`, `cancelled` y `recovery`. `ERROR`/`timeout` en una
evaluación describen el resultado del evaluador, no un nuevo `Snapshot.state`.
La implementación puede añadir `Is(target error) bool` para que el `Kind`
responda a los cinco sentinels sin comparar mensajes; `Cause` sigue reservado
para `context`/Runtime y no sustituye al sentinel de la decisión.

La estructura es conceptual y no se añade en esta tarea. `Cause` conserva
`context.DeadlineExceeded`, `context.Canceled`, `runtime.ErrUnavailable` o
`runtime.ErrRecoveryRequired` cuando corresponda. `ErrAdmissionCapacity` es la
decisión preventiva; `runtime.ErrCapacity` sigue representando un fallo real
de `Start`. `ErrAdmissionRecovery` debe adaptarse al sentinel de Recovery que
ya use la capa que lo emita, no crear una segunda autoridad.

### 6.2 Precedencia y deadlines

Se calculan deadlines absolutos desde el momento en que se acepta la solicitud
para espera de admisión, sin modificar el contrato existente del proxy:

```text
D_admission = inicio de admisión + capacity.admission.wait_timeout
D_proxy     = deadline del proxy.queue_wait_timeout según su semántica actual
D_context   = deadline absoluto de request.Context, si existe
D_effective = el mínimo de los deadlines presentes
```

`D_effective` incluye el tiempo consumido por evaluación, saturación del pool y
reevaluación. `D_proxy` es el deadline que ya crea el proxy con
`context.WithTimeout(r.Context(), proxy.queue_wait_timeout)`; `D_context` puede
ser anterior y siempre gana. `D_admission` no extiende `D_proxy`, `D_proxy` no
extiende el contexto y ningún deadline se reinicia por reevaluar o por replay
idempotente. Si una ruta administrativa no tiene contexto de espera, se aplica
el deadline de admisión; si solo existe uno, ese es el límite. La cancelación
explícita de la lease y el shutdown no esperan al deadline.

En la comprobación final serializada, la precedencia es:

1. Recovery requerido, lease terminal, cancelación confirmada o shutdown:
   interrumpen y descartan cualquier `FIT`.
2. Si `D_effective` venció antes de ese commit, termina como timeout; un `FIT`
   llegado después solo queda como resultado tardío.
3. Solo una lease aún `QUEUED`, no cancelada, no expirada, sin Recovery y con
   `D_effective` vigente puede usar un `FIT` para pasar a la comprobación final
   y luego a `PREPARING`.

Si timeout y cancelación son observables en la misma ventana, gana el evento
que el Scheduler confirma primero bajo su lock; si la cancelación ya estaba
confirmada, el error es cancelación, y si el deadline ya estaba vencido en el
commit, el error es timeout. Si Recovery fue confirmado primero, el resultado
es Recovery. En ningún caso gana un resultado `FIT` que solo llegó fuera de
esa transición. Un `FIT` que llega antes del vencimiento pero espera a adquirir
el lock debe volver a comprobar el deadline en el commit.

`capacity.admission.wait_timeout` y `proxy.queue_wait_timeout` son límites de
espera configurables y distintos. No se extiende el TTL de una lease `ACTIVE`,
no se introduce heartbeat para `QUEUED` y el shutdown cancela la espera usando
la ruta de cleanup existente.

### 6.3 Traducción sin romper APIs existentes

La traducción futura debe reutilizar respuestas y estados ya existentes; no se
introducen códigos HTTP nuevos ni un schema OpenAI nuevo:

| Error interno | API administrativa | API OpenAI-compatible | CLI |
|---|---|---|---|
| `ErrAdmissionCapacity` | Mientras la lease siga `QUEUED`, conserva `202` y su estado actual; si termina, reutiliza la rama de conflicto existente (`409`, igual que `scheduler.ErrCapacity`, donde hoy aplica). | El rechazo previo al upstream reutiliza la rama actual de conflicto de Scheduler; no cambia la respuesta mientras `enabled=false`. | Conserva el formato de error HTTP actual y el código de salida existente. |
| `ErrAdmissionTimeout` | Reutiliza el estado terminal y cuerpo de lease existentes; no crea un status nuevo. | Para la espera de cola, conserva `504 Gateway Timeout` ya usado por `WaitActive`; el cuerpo sigue el formato OpenAI actual. | Se clasifica como timeout de respuesta, sin nuevo exit code. |
| `ErrAdmissionUnavailable` | Reutiliza el mapeo actual de disponibilidad (`503` cuando corresponde). | `503` existente; no se presenta como `FIT`. | Mensaje existente de servicio no disponible. |
| `ErrAdmissionCancelled` | `CANCELLED` por la operación actual; si el cliente ya cerró, no se inventa una respuesta. | Antes de headers/upstream, conserva la respuesta de cancelación/contexto actual; después de comenzar SSE no puede cambiar status y solo registra/cierra según el contrato SSE vigente. | Error genérico actual si la operación administrativa recibió respuesta. |
| `ErrAdmissionRecovery` | `503` y `RECOVERY_REQUIRED` con la respuesta existente. | `503` existente. | Clasificación actual de Recovery. |

Con `admission.enabled=false`, no se emiten estos errores preventivos: las
respuestas, status HTTP, headers, streaming y SSE son idénticos al flujo
actual. La forma exacta de un cuerpo de error futuro requiere revisar cada
endpoint antes de activarlo. En particular, una decisión de admisión ocurre
antes de iniciar upstream/SSE; nunca se reescribe una respuesta después de
haber enviado headers o el primer evento.

### 6.4 Eventos y reinicio

| Evento | Resultado de política | Garantía requerida |
|---|---|---|
| Planner devuelve error, panic o timeout | `UNKNOWN` con estado de observación `ERROR`; aplicar `on_unknown`. | El Scheduler no espera indefinidamente ni se cae. |
| Snapshot `STALE`/`UNAVAILABLE` | `UNKNOWN`, nunca `FIT`. | Se conserva estado, antigüedad, fuente y justificación. |
| Runtime devuelve `ErrCapacity` | Fallo operativo actual; no reintentar sin límite. | No falsear una evaluación anterior. |
| Lease expirada | `EXPIRED`/conflicto según contrato existente; no activar. | Resultado tardío no revive lease. |
| Cancelación concurrente | Cancelar espera y retirar/cerrar por la autoridad actual. | No iniciar proceso después de confirmar cancelación. |
| Recovery bloqueado | `RECOVERY_REQUIRED` y no admisión. | Precede a capacidad y no se resuelve automáticamente. |
| Shutdown | Cancelar espera y seguir el cleanup actual. | No dejar worker, proceso o lease en estado ficticio. |
| Reinicio durante espera | La cola en memoria no se restaura; registros no terminales/idempotencia quedan sujetos a Recovery. | No reanudar ni duplicar la carga, no adoptar procesos. |

No se añade persistencia para evaluaciones o esperas. Si un futuro diseño
requiere sobrevivir al reinicio, necesitará una aprobación y un contrato de
Recovery separado; no se puede inferir desde el historial shadow.

## 7. Política de snapshots

El Planner no completa ni corrige datos faltantes. La clasificación mínima es:

| Snapshot | Regla | Veredicto admisible |
|---|---|---|
| `AVAILABLE` | Medición válida, con dominios requeridos presentes y `age <= max_snapshot_age`. | Puede producir `FIT` o `INSUFFICIENT`. |
| `STALE` | Supera `max_snapshot_age`; no es evidencia actual. | `UNKNOWN`. |
| `UNAVAILABLE` | El proveedor no pudo medir. | `UNKNOWN`. |
| `PARTIAL` | Solo se evalúa si existe evidencia suficiente para todos los dominios de memoria requeridos por la operación. Si falta un dominio requerido, no se rellena. | `FIT`/`INSUFFICIENT` solo con evidencia suficiente; de lo contrario `UNKNOWN`. |
| `PERMISSION_DENIED` | Impide determinar la capacidad necesaria. | `UNKNOWN`. |

Los bytes faltantes son `null`, nunca cero. En Apple Silicon se conserva el
presupuesto de memoria unificada. En NVIDIA se mantienen separados RAM y VRAM;
la ausencia de VRAM solo es admisible cuando la operación no requiere ese
dominio y existe evidencia suficiente del resto. No se inventan valores,
capacidad libre ni consumo incremental.

`max_snapshot_age=6s` es un valor inicial configurable y pendiente de
validación operativa. El estado `AVAILABLE` no convierte un snapshot en una
reserva ni elimina TOCTOU.

## 8. Riesgos de concurrencia y mitigaciones

| Riesgo | Mitigación contractual |
|---|---|
| TOCTOU entre snapshot y `llama-server` | Snapshot con timestamp/edad; reevaluación en cabeza FIFO y comprobación final justo antes de `Start`. Aun así, FIT no es garantía. |
| Dos solicitudes evaluadas simultáneamente | Un único worker decide en orden. No sumar reservas ficticias; la segunda se reevalúa después de la primera. Procesos LARM separados no comparten garantía. |
| Head-of-line blocking | `INSUFFICIENT` no permite saltar la cabeza. `wait_timeout` y cancelación liberan la cola; medir explícitamente la latencia. |
| Expiración durante espera | No usar lease `ACTIVE` para esperar. Verificar estado y expiración antes de cada reevaluación y antes de activar. |
| Cancelación concurrente | Operación idempotente del Scheduler; el commit final comprueba cancelación y lease bajo el mismo lock. |
| Residencia cambia después de evaluar | Comparar `ProcessInstanceID`, identidad de inicio y `READY`; si cambia, reclasificar. |
| Reinicio de LARM | No restaurar cola, lease ni proceso; Recovery sigue fail-closed y los registros no terminales no se reutilizan. |
| Snapshot `STALE` o `UNAVAILABLE` | `UNKNOWN`; nunca convertir bytes nulos o proveedor ausente en cero/FIT. |
| Fallo del Planner | Timeout acotado, panic aislado y `UNKNOWN`; la política decide allow/reject/wait sin mutar Scheduler desde Planner. |
| Procesos externos cambian memoria | Justificación y limitación explícitas; no hay garantía de exclusividad ni reserva ficticia. Un fallo real de arranque sigue siendo posible. |
| `RESIDENT_REUSE` con PID reutilizado | PID no es identidad; exigir `ProcessInstanceID` e identidad OS viva desde Runtime. |
| Crash después de aceptar pero antes de activar | La adquisición existente queda sujeta a Recovery; no asumir exactly-once ni relanzar automáticamente. |

## 9. Configuración propuesta (no operativa en esta tarea)

Ejemplo conceptual para una futura versión del parser:

```yaml
capacity:
  admission:
    enabled: false
    on_insufficient: wait
    on_unknown: allow
    wait_timeout: "5m"
    reevaluation_interval: "2s"
    max_snapshot_age: "6s"
```

Compatibilidad y validación propuesta:

- Si `admission` está ausente, `enabled=false`; el comportamiento debe ser
  byte a byte equivalente en decisiones observables al actual.
- Defaults conceptuales iniciales: `enabled=false`, `on_insufficient=wait`,
  `on_unknown=allow`, `wait_timeout=5m`, `reevaluation_interval=2s` y
  `max_snapshot_age=6s`. Todos son configurables y requieren validación
  operativa antes de activar enforcement.
- Validar enums, duraciones parseables y positivas, `wait_timeout` finito, y
  que la reevaluación no sea cero. Rechazar valores desconocidos con
  `KnownFields`, como el parser actual.
- `shadow_mode` conserva su semántica existente y es independiente de
  `admission.enabled`. Shadow habilitado no implica enforcement.
- No añadir estas claves a `internal/config` ni a YAML operativo hasta aprobar
  el contrato y la matriz. En el checkout actual, el ejemplo completo sería
  rechazado por el decoder estricto si se copia sin una futura modificación.

## 10. Diferencias respecto al comportamiento actual

| Tema | Actual | Diseño futuro |
|---|---|---|
| Decisión | Scheduler siempre sigue su flujo; Planner/Shadow no bloquean. | Evaluación en cabeza FIFO podría permitir espera/rechazo. |
| `INSUFFICIENT` | Solo dato histórico. | No iniciar nueva carga; esperar acotadamente o terminar. |
| `UNKNOWN` | No afecta operación. | Igual por defecto, con riesgo explícito y política configurable. |
| Lease en espera | Lease `QUEUED`; TTL empieza al activarse. | Mantener espera lógica no activa y con límite; nunca lease `ACTIVE` prolongada. |
| Resident reuse | Shadow lo etiqueta `FIT` sin validar identidad; Scheduler reutiliza si Runtime dice READY. | La política debe validar identidad viva y Recovery antes de reutilizar. |
| Evaluación | `capacity.Evaluation` no incluye lease, edad estructurada ni identidad residente. | Contrato ampliado conceptual; no se modifica la API actual en esta tarea. |
| Reinicio | Cola/historial shadow son efímeros; leases pendientes durables quedan Recovery Required. | La espera de capacidad no se reanuda automáticamente. |

## 11. Decisiones aprobadas, valores iniciales y pendientes

### Aprobadas para el diseño

1. La espera usa la cabeza lógica FIFO, lease `QUEUED` y fase interna
   `WAITING_CAPACITY`; no hay `PREPARING` mientras falta capacidad.
2. No hay `skip`, reservas ficticias, extensión de TTL ni heartbeat para
   `QUEUED`; cancelación, timeout, Recovery y shutdown pueden interrumpir.
3. El evaluador es read-only, acotado por contexto y concurrencia, y sus
   resultados tardíos no autorizan activaciones.
4. `UNKNOWN` no se presenta como `FIT`; el default de política futura es
   `allow` para conservar compatibilidad.
5. `RESIDENT_REUSE` permanece diferenciada y exige identidad OS,
   `ProcessInstanceID`, `READY`, modelo, ownership, Recovery, Execution Gate y
   lease válidos.
6. Snapshots stale, unavailable, insuficientes o con permisos denegados son
   `UNKNOWN`; no se inventan bytes y se distinguen memoria unificada y RAM/VRAM.
7. Las APIs reutilizan estados, cuerpos y códigos existentes; no se agregan
   códigos HTTP en este contrato.

### Valores iniciales propuestos

`enabled=false`, `on_insufficient=wait`, `on_unknown=allow`, `wait_timeout=5m`,
`reevaluation_interval=2s` y `max_snapshot_age=6s`. Son configurables y no
están aprobados como configuración operativa hasta validarlos con métricas,
latencia de arranque, presión de memoria y carga concurrente.

### Riesgos aceptados para el diseño

- `FIT` no es una reserva ni garantía frente a TOCTOU, procesos externos o
  consumo real de llama.cpp.
- Dos instancias LARM no comparten una reserva de hardware; el contrato local
  no resuelve coordinación entre procesos.
- `RESIDENT_REUSE` requiere un modelo futuro de memoria incremental y no puede
  asumir que esa memoria sea cero.
- Un reinicio no reanuda automáticamente una espera ni adopta procesos; se
  conserva Recovery fail-closed.

### Requiere validación/aprobación antes de activar enforcement

- valores operativos y capacidad máxima del evaluador;
- métricas/alertas obligatorias para `UNKNOWN` con `on_unknown=allow`;
- memoria incremental de inferencia y criterio de dominios requeridos;
- evidencia de plataforma macOS arm64 y fixtures Linux/NVIDIA;
- comportamiento exacto de cada endpoint administrativo, OpenAI y SSE ante
  timeout/cancelación, sin romper compatibilidad;
- coordinación o prohibición explícita de múltiples instancias LARM sobre el
  mismo hardware.

Hasta completar esa validación, `admission.enabled` debe permanecer
deshabilitado y Shadow Mode debe continuar siendo solo observación. Ninguna de
las decisiones conceptuales anteriores representa una funcionalidad
implementada.
