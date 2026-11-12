# LARM-007.3.2a — Matriz de pruebas de admisión

## Alcance y regla de estado

Esta matriz define pruebas de caracterización y gates para una futura política
de admisión basada en memoria. No añade pruebas al árbol de código en esta
tarea. `admission.enabled` no existe todavía en el parser operativo y ningún
caso de enforcement puede marcarse `PASS`.

Estados usados abajo (el primero es la clasificación principal del caso):

- **Caracterización actual:** puede verificarse contra el código existente sin
  afirmar enforcement.
- **Pendiente de enforcement:** debe permanecer `NOT IMPLEMENTED` hasta que se
  apruebe e implemente la política.
- **Gate de regresión:** cuando exista la implementación, debe pasar además de
  las pruebas actuales; no sustituye `go test ./...` ni `go test -race ./...`.
- **Validación de plataforma:** caracteriza proveedores y fixtures de una
  plataforma; no demuestra enforcement. Si el caso también ejercita admisión,
  esa parte permanece `NOT IMPLEMENTED`.

La aceptación de un caso futuro exige simultáneamente el resultado esperado,
la ausencia de cambios no autorizados en FIFO/leases/Recovery y evidencia de
los IDs/timestamps relevantes. No se requiere hardware NVIDIA: los casos
NVIDIA usan fixtures.

## Matriz ADMISSION-01–ADMISSION-25

La columna **estado** separa lo que puede ser `PASS` en el checkout actual de
lo que debe permanecer `NOT IMPLEMENTED`. Un resultado de fixture o de
caracterización nunca convierte enforcement futuro en `PASS`.

| ID / estado | Precondiciones | Estímulo | Resultado esperado | Criterio de aceptación |
|---|---|---|---|---|
| **ADMISSION-01**<br>Ahora: caracterización | Modelo configurado, nueva carga, snapshot `AVAILABLE` con memoria suficiente, Recovery normal. | Solicitar `NEW_LOAD` con request/lease identificables. | Planner puede producir `FIT`; no autoriza activación en el checkout actual. | Evidencia contiene modelo, request, operación, snapshot, edad, bytes, fuente, justificación y timestamp. Enforcement: `NOT IMPLEMENTED`. |
| **ADMISSION-02**<br>Ahora: caracterización; futuro: enforcement | Nueva carga; snapshot `AVAILABLE` con memoria menor al requerimiento. | Evaluar el elemento en cabeza FIFO. | Planner produce `INSUFFICIENT`; bloquear `Start` es comportamiento futuro. | No se inventa reserva ni decremento de memoria. La espera/rechazo futuro permanece `NOT IMPLEMENTED`. |
| **ADMISSION-03**<br>Ahora: caracterización + gate | Nueva carga; snapshot `UNAVAILABLE`, bytes nulos o Planner sin metadata. | Evaluar. | `UNKNOWN`, nunca `FIT`; con `on_unknown=allow` futuro, el flujo actual continúa. | Causa visible y Shadow/Scheduler no bloqueantes. Enforcement: `NOT IMPLEMENTED`. |
| **ADMISSION-04**<br>Ahora: caracterización; futuro: enforcement | Modelo `READY`, proceso administrado con identidad viva, solicitud del mismo modelo. | Observar `RESIDENT_REUSE` y activar por la ruta actual. | El comportamiento actual puede reutilizar; el contrato futuro exige identidad, ownership, Recovery, Execution Gate y lease válidos. | `ProcessInstanceID`, identidad de inicio, modelo y estado se registran. La validación de admisión es `NOT IMPLEMENTED`. |
| **ADMISSION-05**<br>Ahora: caracterización + gate | Una lease activa/preparando o una solicitud delante; segunda solicitud en cola. | Observar la segunda como `QUEUED`. | Shadow registra sin cambiar FIFO ni esperar al Planner. | `queue_position` y orden no cambian; no se marca enforcement de capacidad como `PASS`. |
| **ADMISSION-06**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | Tres solicitudes A, B, C en FIFO; A tiene memoria insuficiente. | Evaluar A y dejarla en `WAITING_CAPACITY`. | B y C no adelantan A; A conserva lease `QUEUED`, no publica `PREPARING`, y reevalúa o termina. | Orden A→B→C, posición lógica estable y ningún trabajo posterior mientras A ocupa la cabeza. |
| **ADMISSION-07**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | A espera por `INSUFFICIENT`; medición posterior pasa a `FIT`; lease sigue no terminal. | Publicar medición y reevaluar A. | A avanza una sola vez a comprobación final y luego a la activación normal. | Nuevo `evaluation_id`/timestamp; no se usa `FIT` antiguo, no se duplica carga y no hay lock durante evaluación. |
| **ADMISSION-08**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | A espera; nuevas mediciones siguen insuficientes. | Reevaluar hasta el límite. | A permanece sin `PREPARING`/`ACTIVE`; al vencer devuelve `ErrAdmissionTimeout` y la lease `QUEUED` termina por la ruta existente, normalmente `CANCELLED`. | Espera finita, sin busy loop, heartbeat ficticio, proceso ni goroutines ilimitadas; FIFO consistente. |
| **ADMISSION-09**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | A espera con `wait_timeout`, `queue_wait_timeout` y deadline de contexto posibles. | Vencer `.a` admisión, `.b` proxy, `.c` contexto o `.d` simultáneamente. | Rige el primer deadline absoluto aplicable; devuelve timeout tipado y no extiende otro contrato. La lease se limpia por la ruta existente. | Se conserva deadline/causa; cancelación y Recovery confirmados interrumpen antes de un `FIT`; una carrera se decide en el commit serializado. |
| **ADMISSION-10**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | A espera; cancelación de contexto/lease concurrente con una reevaluación `FIT`. | Cancelar y entregar `FIT` en la misma ventana. | Gana cancelación si se confirma antes del commit; no hay `Start` posterior. | Tie-break definido por commit serializado; operación idempotente, sin huérfanos ni proceso. |
| **ADMISSION-11**<br>Ahora: gate; futuro: enforcement | Lease `ACTIVE` con TTL corto; otra solicitud espera capacidad. | Dejar expirar la lease activa durante la espera. | La lease expirada no es permiso; la siguiente solicitud sigue residencia/cola normales. | No se renueva lease por esperar, no hay heartbeat `QUEUED` y no se reactiva una terminal. |
| **ADMISSION-12**<br>Ahora: caracterización + gate | Lease externa `QUEUED` o `PREPARING`; heartbeat solo aplica a `ACTIVE`. | Enviar heartbeat antes de `ACTIVE` y después de expirar. | Se conserva conflicto/expiración actual; evaluación no cambia estados. | No se mantiene una lease `ACTIVE` artificial durante espera. |
| **ADMISSION-13**<br>Ahora: caracterización + gate; futuro: enforcement | `AdmissionGate`/Registry o Runtime en `RECOVERY_REQUIRED`; snapshot `FIT`. | Solicitar nueva carga o reutilización. | Recovery precede a `FIT`; no se admite ni se inicia. | El bloqueo actual sigue antes de `Start`, sin adoptar ni detener proceso. Rama de admisión: `NOT IMPLEMENTED`. |
| **ADMISSION-14**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | A espera; LARM termina abruptamente antes de reevaluar. | Reiniciar con store sucio o estado no terminal. | Cola no se reanuda; Recovery bloquea y no relanza. | No duplicación/restauración de lease o proceso; diagnóstico con IDs históricos y fail-closed. |
| **ADMISSION-15**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | Snapshot `FIT`; A se cancela o se reevalúa antes de terminar la evaluación anterior. | Entregar evaluación tardía o consumir memoria entre evaluación y `Start`. | Resultado tardío se descarta; fallo real sigue siendo Runtime/carga. | Ningún `FIT` tardío autoriza `Start`; se registra TOCTOU sin afirmar garantía de capacidad. |
| **ADMISSION-16**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | A y B son nuevas cargas en la misma instancia; pool/cola del evaluador acotados. | Evaluarlas concurrentemente y saturar el límite. | Worker decide FIFO; B espera/reintenta acotadamente o recibe `UNKNOWN`, sin reserva ficticia. | Límite verificable, sin goroutines ilimitadas ni dos `Start` fuera de garantías; FIFO A→B. |
| **ADMISSION-17**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | `RESIDENT_REUSE` observado con `ProcessInstanceID=P1`; antes de ejecutar cambia a P2/no `READY`. | Activar con evaluación de P1. | Se rechaza reutilización silenciosa y se reclasifica como nueva carga o `UNKNOWN`. | Mismatch detectado; no se elude Recovery ni la lease. |
| **ADMISSION-18**<br>Ahora: caracterización + gate | Observer con intervalo conocido; snapshot más antiguo que `max_snapshot_age` (o tres intervalos actual). | Evaluar nueva carga. | `STALE` produce `UNKNOWN`, no `FIT`. | Edad, estado y fuente visibles; Shadow no bloquea y Scheduler no cambia. |
| **ADMISSION-19**<br>Ahora: NOT IMPLEMENTED; futuro: enforcement | Planner retorna panic, error o no responde; subcaso `.a` llena el pool y `.b` cancela. | Ejecutar evaluación y cancelar una evaluación en curso. | `UNKNOWN` con causa conservada, `on_unknown` aplicable y worker estable. | Timeout, concurrencia y cola acotados; sin acumulación de goroutines ni mutación desde `capacity`. |
| **ADMISSION-20**<br>Ahora: caracterización | `capacity.shadow_mode=true`, Planner fixture y Scheduler normal. | Crear solicitudes `NEW_LOAD`, `RESIDENT_REUSE` y `QUEUED`. | Shadow registra resultados async; ninguna decisión operativa cambia. | Historial incluye operación/veredicto/fuente/justificación; enforcement no puede ser `PASS`. |
| **ADMISSION-21**<br>Ahora: caracterización + gate | `capacity.shadow_mode=false`, misma configuración y solicitudes. | Repetir ADMISSION-20. | No se instala observer Shadow; FIFO/leases/lifecycle no cambian. | Diferencia solo observacional; sin endpoint ni mutación adicional. |
| **ADMISSION-22**<br>Ahora: gate de regresión | Configuración actual sin `admission.enabled` (equivalente al futuro `false`). | Secuencias nuevas, resident reuse, cancelación, expiry y Recovery existentes. | Resultados idénticos al baseline. | Comparar estados, posiciones, errores, Runtime y procesos; cualquier diferencia es regresión. |
| **ADMISSION-23**<br>Ahora: gate de regresión; futuro: enforcement | Cliente OpenAI compatible con/sin `X-LARM-Lease-ID`; HTTP y SSE. | `.a` chat/embeddings no streaming; `.b` chat streaming SSE; `.c` timeout/cancelación en cola y durante SSE. | Se conservan leases, HTTP, headers, JSON de error y reglas SSE actuales cuando admission está deshabilitado. | Sin headers/endpoints nuevos; espera usa timeout actual; tras headers/primer evento no se reescribe status. Enforcement: `NOT IMPLEMENTED`. |
| **ADMISSION-24**<br>Ahora: validación de plataforma; futuro: NOT IMPLEMENTED | Fixture Linux con RAM y `nvidia-smi` sintéticos: VRAM suficiente/insuficiente, `PARTIAL` y proveedor ausente. | `.a` evaluar `gpu_layers` y verificar dominios; `.b` intentar política de espera/bloqueo. | `.a` separa RAM/VRAM y clasifica según evidencia; `.b` no bloquea ni espera en este checkout. | No GPU real; no se inventa VRAM; `PARTIAL` sin todos los dominios requeridos es `UNKNOWN`; `.a` puede ser `PASS`, `.b` es `NOT IMPLEMENTED`. |
| **ADMISSION-25**<br>Ahora: validación de plataforma; futuro: NOT IMPLEMENTED | macOS arm64 con memoria unificada; sin proveedor NVIDIA. | `.a` evaluar nueva carga, `RESIDENT_REUSE`, stale/unavailable; `.b` intentar enforcement. | `.a` usa un presupuesto unificado; ausencia de GPU dedicada no causa `UNKNOWN` por sí sola; stale/unavailable sí. `.b` no activa política. | Validar provider Darwin y `architecture: unified`, sin modelo real/NVIDIA; `.a` puede ser `PASS`, `.b` es `NOT IMPLEMENTED`. |

## 3. Fixtures y aislamiento

### 3.1 Fixtures comunes

- Planner fake que devuelve `FIT`, `INSUFFICIENT`, `UNKNOWN`, error, panic y
  bloqueo hasta `context.Done()`.
- Snapshots deterministas con `AVAILABLE`, `STALE`, `UNAVAILABLE`, estados de
  sistema/GPU, `measured_at` y bytes nulos explícitos.
- GGUF fixture pequeño ya utilizado por `internal/capacity`; no descargar ni
  iniciar un modelo real.
- Runtime fake que registra `Start`, `Stop`, identidad de proceso y el orden de
  llamadas, con fallos inyectables de capacidad y Recovery.
- Reloj o tiempos controlables para timeout, expiración, cancelación y TOCTOU;
  no depender de sleeps largos.
- Scheduler/lease fixture con `max_queue_size`, TTL corto solo para pruebas y
  persistencia existente; no crear un store de admisión nuevo.

### 3.2 Linux/NVIDIA

ADMISSION-24 debe usar la misma forma de datos que `memory.GPUResult` y los
fixtures de `internal/capacity/planner_test.go`. Debe probar proveedor
`AVAILABLE` y proveedor ausente/parcial. La etiqueta “NVIDIA” describe el
fixture, no una validación de hardware ni un requisito de CI.

### 3.3 macOS arm64

ADMISSION-25 usa el provider Darwin y sus pruebas disponibles. Debe comprobar
`architecture: unified`, el presupuesto único y que `gpu_provider` ausente no
sea tratado como VRAM cero. No requiere privilegios ni ejecución de
`llama-server`.

## 4. Evidencia y comandos cuando se implemente

Para este entregable solo se debe validar documentación:

```bash
git diff --check
```

Si una futura iteración agrega pruebas de caracterización, además debe ejecutar
sin hardware especial:

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
git diff --check
```

La evidencia debe separar:

1. `PASS` de contratos actuales (FIFO, leases, Recovery, Shadow, Planner y API
   OpenAI sin enforcement).
2. `NOT IMPLEMENTED` de los casos que exigen bloquear, esperar, reevaluar o
   cancelar por capacidad.
3. `FAIL`/`UNKNOWN` del Planner como datos esperados, no como fallos de
   infraestructura.

No se debe marcar como `PASS` la parte de enforcement de ADMISSION-02, 04,
06–10, 11, 13–17, 19, 23, 24.b o 25.b por el mero hecho de que exista un
fixture. Las partes de caracterización actual, gates y validación de plataforma
pueden tener `PASS` independiente cuando su evidencia corresponda.

## 5. Gates de no regresión

Una implementación futura no puede ser aprobada si cambia cualquiera de estos
invariantes:

- el orden FIFO global o la posición observable de la cola;
- el inicio del TTL al pasar a `ACTIVE`, heartbeat y `max_lease_duration`;
- la prohibición de liberar leases con inferencias activas/ambiguas;
- el bloqueo fail-closed de Recovery y la no adopción post-crash;
- la identidad `ProcessInstanceID`/OS frente a un PID reutilizado;
- la semántica actual de `RESIDENT_REUSE`, residencia, precarga y Runtime;
- el comportamiento OpenAI-compatible cuando admission está deshabilitado;
- el hecho de que Shadow Mode no espera ni consulta el resultado del Planner;
- la ausencia de reservas ficticias o persistencia nueva.

La decisión de activar enforcement queda fuera de esta matriz y requiere
aprobación explícita de los valores y validaciones pendientes del contrato.
