# LARM-007.3.7a.1 — Hardening del plan experimental físico en Apple Silicon

## 1. Estado y alcance

Este documento **revisa y corrige** el plan experimental de LARM-007.3.7a contra
los contratos que existen hoy en el código. Es documentación de validación; no
autoriza por sí sola una ejecución.

No se ejecutaron modelos reales, cargas GGUF, LARM, `llama-server` ni pruebas de
presión. Este documento no modifica configuración operativa ni implementación.

### Contratos verificados

- `POST /api/v1/leases` acepta exactamente un objeto JSON con `model`
  (`string`, obligatorio); rechaza campos desconocidos, cuerpos mayores de 4096
  bytes y más de un objeto JSON. Sin `Idempotency-Key`, responde `202` con el
  estado de lease. Con esa cabecera debe ser un UUID canónico; un replay de
  una lease `ACTIVE` responde `200`.
- `DELETE /api/v1/leases/{id}` no lleva cuerpo; responde `200` con el estado
  de lease. `QUEUED` pasa a `CANCELLED`, `PREPARING` a `CANCELLED` y `ACTIVE`
  a `COMPLETED`. Repetir la liberación de un estado terminal es idempotente.
  Una lease protegida por inferencia devuelve `409`.
- El Planner expone `GET /api/v1/capacity/{model}` y
  `GET /api/v1/capacity?model={model}`. La respuesta contiene `verdict`,
  `snapshot`, `estimate`, `capacity`, `justification` y `limitations`.
  `FIT`, `INSUFFICIENT` y `UNKNOWN` son veredictos; `FIT` significa que la
  **estimación** cabe en la instantánea disponible, no que Runtime vaya a
  activarse.
- `GET /api/v1/processes` devuelve un arreglo de `ProcessInstance`, no un
  objeto contenedor. Cada elemento identifica `process_instance_id`, `model_id`,
  `pid`, `state`, `ownership_status`, `process_identity_status` y demás
  evidencia de proceso. La identidad viva se revalida al listar; PID solo no
  prueba identidad.
- `GET /api/v1/status` incluye `memory.snapshot`. `GET /api/v1/models` incluye
  `memory` por modelo cuando hay una observación. `/api/v1/admission/metrics` y
  `/api/v1/admission/events` exponen contadores y eventos recientes de
  Admission; el buffer de eventos es acotado.
- `RESIDENT_REUSE` exige un modelo `READY`, proceso vivo y administrado con
  modelo, `ProcessInstanceID`, identidad de inicio y ownership coherentes. No
  calcula una nueva carga ni contabiliza de nuevo el GGUF. En la evaluación
  actual devuelve `FIT` por identidad residente; no mide el workspace o KV
  cache incremental de una inferencia.
- Liberar una lease **no implica detener inmediatamente** un proceso. Sin
  `keep_alive` ni `time_alive`, el Scheduler solicita la descarga cuando el
  proceso queda ocioso; con `time_alive` espera la residencia; con
  `keep_alive` conserva el proceso. La parada es asíncrona y debe observarse por
  `state`, `pid`, `process_instance_id` y estado de terminación.

## 2. Inventario corregido de modelos

Los nombres de modelo y las rutas se fijan contra la configuración revisada; no
se deben copiar rutas de un plan anterior.

| ID contractual | GGUF | Ruta exacta | Contexto operativo | Uso en el plan |
|---|---|---|---:|---|
| `code-fast` | `Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf` | `/Users/odin/AI/runtime/models/Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf` | 16384 | E1–E7, E9 |
| `code-agent` | `Qwen3-Coder-30B-A3B-Instruct-Q4_K_M.gguf` | `/Users/odin/AI/runtime/models/Qwen3-Coder-30B-A3B-Instruct-Q4_K_M.gguf` | 32768 | E1–E7, E9 |
| `commit` | `Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf` | `/Users/odin/AI/runtime/models/Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf` | 16384 | Solo si la configuración autorizada contiene exactamente este ID y ruta |

`code-fast` y `commit` pueden apuntar al mismo archivo, pero son contratos de
modelo distintos: el identificador enviado en la lease es el ID configurado, no
el nombre del GGUF. Antes de cada fase se debe confirmar:

```sh
BASE_URL="${BASE_URL:-http://127.0.0.1:8090}"
MODEL_ID="code-fast"                 # usar el ID realmente configurado
GGUF_PATH="/Users/odin/AI/runtime/models/Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf"
test -f "$GGUF_PATH"
```

No usar `qwen25-coder14b`, `qwen38-27b` ni otros alias históricos salvo que
`GET /api/v1/models` demuestre que son IDs configurados actuales.

## 3. Configuraciones aisladas por escenario

Estas son **configuraciones de prueba**, no cambios que deba realizar esta
tarea. Cada una requiere un proceso LARM aislado y debe conservar los
identificadores y rutas verificados en la sección 2.

| Config | Política de Admission | residence | Other settings | Escenarios |
|---|---|---|---|---|
| `CFG-WAIT` | `enabled: true`, `on_insufficient: wait`, `on_unknown: allow`, `wait_timeout: 2m`, `reevaluation_interval: 2s`, `max_snapshot_age: 6s` | `keep_alive: false`, `time_alive: 0s` | `max_active_models: 1`, `max_queue_size: 4` | E2, E6 |
| `CFG-REJECT` | `enabled: true`, `on_insufficient: reject`, `on_unknown: reject`, `wait_timeout: 30s`, `reevaluation_interval: 2s`, `max_snapshot_age: 6s` | `keep_alive: false`, `time_alive: 0s` | `max_active_models: 1`, `max_queue_size: 4` | E3 |
| `CFG-RESIDENT` | `enabled: true`, `on_insufficient: reject`, `on_unknown: reject` | `keep_alive: true`, `time_alive: 0s` | un solo modelo | E4 |
| `CFG-TIMEALIVE` | `enabled: true`, `on_insufficient: reject`, `on_unknown: reject` | `keep_alive: false`, `time_alive: 30s` | un solo modelo | E5 |
| `CFG-PLAIN` | `enabled: false` | `keep_alive: false`, `time_alive: 0s` | un solo modelo | E1, E7, E9 |
| `CFG-PRESSURE` | `enabled: true`, `on_insufficient: reject`, `on_unknown: reject` | `keep_alive: false`, `time_alive: 0s` | límites explícitos de E8 | Solo E8, con aprobación independiente |

Reglas de aislamiento:

1. `wait` y `reject` no se mezclan. `wait` conserva la lease en `QUEUED`, en la
   cabeza FIFO, y reevalúa; `reject` termina la lease por la política de
   Admission. Un timeout de espera tampoco es una prueba de `reject`.
2. `on_unknown=reject` evita que una medición no disponible se interprete como
   permiso. Para las fases de seguridad de E8 es obligatorio.
3. Ningún escenario puede confiar en la configuración operativa actual. Antes de
   iniciar, registrar el YAML efectivo, el fingerprint y el hash del archivo.
4. No cambiar `keep_alive`/`time_alive` mientras una lease o inferencia esté
   activa.
5. Los endpoints administrativos de start/stop solo existen en el socket Unix
   protegido. Este plan **no los usa**.

## 4. Matriz E1–E9

| ID | Objetivo | Config | Sujeto / modelo | Operación | Evidencia obligatoria | PASS | FAIL / aborto |
|---|---|---|---|---|---|---|---|
| E1 | Línea base de contrato sin enforcement | `CFG-PLAIN` | un modelo y su GGUF verificados | consulta del Planner; una lease; consultar lease, procesos, status y memoria | snapshot vigente; lease trazable; proceso con identidad; métricas antes/después | Respuestas coinciden con los campos documentados; IDs y GGUF coinciden; no hay `INSUFFICIENT` accidental | Cualquier endpoint/schema inesperado, GGUF/ID distinto, o estado de Recovery |
| E2 | Capacidad insuficiente con espera | `CFG-WAIT` | modelo cuyo requisito estimado exceda el presupuesto de la prueba | POST; polling de lease y eventos | lease permanece `QUEUED`; no aparece proceso del modelo nuevo; eventos de evaluación/espera | `INSUFFICIENT` preservado; lease `QUEUED`; sin `PREPARING`; timeout o cancelación terminal explícita | Se activa pese a `INSUFFICIENT`, cambia de veredicto, o se confunde espera con rechazo |
| E3 | Rechazo preventivo | `CFG-REJECT` | mismo sujeto que E2 | POST y polling | lease terminal y error/evento de rejection | No hay proceso nuevo; la lease termina con error de capacidad; no se reintenta automáticamente | Se activa el proceso, o la respuesta HTTP contradice la política |
| E4 | `RESIDENT_REUSE` | `CFG-RESIDENT` | modelo ya `READY` | segunda lease para el mismo modelo; consulta de eventos | `operation=RESIDENT_REUSE`; mismo `process_instance_id`; identidad viva | Segunda lease no crea un `ProcessInstance`; la operación observada es `RESIDENT_REUSE` | Nuevo proceso, identidad stale/reutilizada, o `NEW_LOAD` para modelo ya residente |
| E5 | Liberación y residencia | `CFG-TIMEALIVE` | modelo listo | DELETE; polling de queue/status/procesos | estados de lease, `residence_remaining`, estado/pid del proceso | Lease `COMPLETED`; proceso permanece mientras `time_alive` vence; después se observa terminación o estado final | Se afirma parada inmediata; lease no terminal; se cierran procesos ajenos |
| E6 | Reevaluación y timeout | `CFG-WAIT` | sujeto insuficiente | POST y polling hasta deadline | eventos de reevaluación, duración y lease terminal | waited/re-evaluated counts increase; original deadline is not reset; terminal cause is timeout | Deadline se reinicia, FIFO se altera, o timeout se reporta como `reject` |
| E7 | Evidencia de precisión del Planner | `CFG-PLAIN` | `code-fast` y `code-agent` | dos consultas read-only y snapshots | dos responses completos, hashes/tamaños, RSS observado | La **estimación** y el **consumo observado** se reportan por separado; RSS no se compara de forma proporcional | Se deriva error porcentual de una relación no contractual; se suma RSS a available bytes |
| E8 | Presión controlada de memoria | `CFG-PRESSURE` | proceso auxiliar exclusivo del experimento, nunca un modelo | vea protocolo E8 | incrementa; snapshots; swap; logs; abort marker | Aborta antes del umbral; memoria liberada; proceso auxiliar terminado; LARM intacto | Umbral cruzado, swap creciente, timeout de guardia, proceso auxiliar vivo, o impacto en apps del usuario |
| E9 | Idempotencia y correlación HTTP | `CFG-PLAIN` | un modelo disponible | POST con UUID; replay; GET; DELETE | response bodies, IDs, status codes | Replay devuelve la misma lease; `409` solo ante conflicto/terminal según contrato; correlación estable | Cambia lease ID, acepta key inválida, o confunde `202`/`200`/`409` |

### Diferencias que deben quedar explícitas

- **Seguridad** es un criterio binario: no se superó ningún umbral de seguridad,
  no hubo arranque bajo presión y la limpieza fue efectiva.
- **Precisión de estimación** es una métrica descriptiva: diferencia entre
  `estimate.required_bytes` y consumo observado, medida solo cuando ambos
  existen. No es un porcentaje contractual porque RSS, memoria privada y mapped
  memory no son equivalentes.
- `FIT` es una autorización de la evaluación para continuar, no una garantía de
  activación. `admitted` mide autorizaciones que llegaron a `PREPARING`, no
  procesos activados.
- Una lease `ACTIVE` sí prueba que Runtime llegó a estado operativo, pero no
  prueba que una inferencia posterior sea correcta.
- El RSS total no es proporcional de forma contractual al tamaño de contexto:
  depende de batching, cuantización, backend, allocator y reutilización de KV
  cache. Solo se reportan observaciones apareadas, nunca una ley de escala.

## 5. Comandos contrastados con la API

Todos los comandos son plantillas. `BASE_URL` debe apuntar al proceso LARM de la
configuración aislada. No incluir `model_id`, `execution_id` ni otros campos no
aceptados por el endpoint.

### 5.1 Inspección read-only

```sh
curl --fail-with-body --silent --show-error "$BASE_URL/health"
curl --fail-with-body --silent --show-error "$BASE_URL/api/v1/models"
curl --fail-with-body --silent --show-error "$BASE_URL/api/v1/status"
curl --fail-with-body --silent --show-error "$BASE_URL/api/v1/processes"
curl --fail-with-body --silent --show-error "$BASE_URL/api/v1/capacity/$MODEL_ID"
curl --fail-with-body --silent --show-error "$BASE_URL/api/v1/admission/metrics"
curl --fail-with-body --silent --show-error "$BASE_URL/api/v1/admission/events"
```

`/api/v1/processes` debe guardarse como arreglo JSON. Para consultar un proceso
se usa el `process_instance_id` devuelto:

```sh
curl --fail-with-body --silent --show-error \
  "$BASE_URL/api/v1/processes/$PROCESS_INSTANCE_ID"
```

### 5.2 Crear una lease

Sin idempotencia:

```sh
curl --fail-with-body --silent --show-error \
  -X POST "$BASE_URL/api/v1/leases" \
  -H 'Content-Type: application/json' \
  --data '{"model":"code-fast"}'
```

Con idempotencia, con clave UUID canónico nueva por intento:

```sh
IDEMPOTENCY_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"
curl --fail-with-body --silent --show-error \
  -X POST "$BASE_URL/api/v1/leases" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  --data '{"model":"code-fast"}'
```

La primera respuesta normal es `202`. Un replay de una lease `ACTIVE` es `200`.
`409` representa conflicto o estado terminal de idempotencia; no debe
interpretarse como el mismo resultado que `503`.

### 5.3 Consultar, heartbeat y liberar

```sh
curl --fail-with-body --silent --show-error \
  "$BASE_URL/api/v1/leases/$LEASE_ID"

curl --fail-with-body --silent --show-error \
  -X POST "$BASE_URL/api/v1/leases/$LEASE_ID/heartbeat"

curl --fail-with-body --silent --show-error \
  -X DELETE "$BASE_URL/api/v1/leases/$LEASE_ID"
```

No usar un JSON de cuerpo en DELETE. Después del DELETE, esperar y observar
`GET /api/v1/queue` y `GET /api/v1/status`; una lease `COMPLETED` no prueba que
el proceso ya haya terminado.

## 6. Rediseño seguro de E8

E8 no carga GGUF, no usa `llama-server` y no altera LARM. Para crear presión de
memoria se necesita una utilidad local separada, aprobada antes de la fase,
que reserve **memoria virtual/anotada de forma nativa**, no memoria Anonymous
comprimible; esto evita el falseo de "reservar memoria" mediante buffer
comprimido. La utilidad y sus umbrales deben existir antes de ejecutar; este
documento no crea ni ejecuta una.

Requisitos obligatorios:

1. Incrementos pequeños: empezar en 64 MiB y avanzar como máximo en pasos de
   64 MiB hasta un tope total preaprobado de 512 MiB.
2. Tope explícito menor o igual a 512 MiB y presupuesto total del experimento
   menor o igual a 1 GiB, verificado antes de arrancar.
3. Guardia exterior con timeout de 60 s por incremento y 5 min por fase.
4. Antes de cada incremento y después de cada uno, registrar
   `system.available_bytes`, `system.used_bytes`,
   `system.swap_used_bytes`, `system.swap_total_bytes`, estado del snapshot y
   RSS del proceso auxiliar.
5. Abortar el incremento inmediatamente si:
   - available cae por debajo de 20% de la RAM total;
   - swap usado aumenta 256 MiB respecto de la línea base o supera 1 GiB;
   - aparece `PRESSURE` o `STALE` en el snapshot;
   - el proceso auxiliar supera 512 MiB RSS/reserva observada;
   - la fase supera su timeout.
6. La interruption automática significa detener **solo** el proceso auxiliar
   exclusivo, liberar su reserva y registrar `ABORTED_E8`. No significa matar
   aplicaciones del usuario.
7. Si el proceso auxiliar no termina en 10 s tras `SIGTERM`, enviar `SIGKILL`
   **solo a ese PID**, después de comprobar que PID, command line y
   `ProcessInstanceID` experimental coinciden con los registrados al inicio.
8. Si no se puede demostrar la exclusividad del PID, no se lo mata: se marca
   `ABORTED_E8_CLEANUP_MANUAL` y se escala al operador.

Comandos ilustrativos del supervisor (deben ser scripts aprobados fuera de este
cambio, no acciones ejecutadas aquí):

```sh
# El supervisor conoce EXPERIMENT_PID y EXPERIMENT_PROCESS_ID en sus metadatos.
# No usar pkill, killall ni patrones de nombre globales.
if [ "${EXPERIMENT_PID:-}" = "" ] || [ "${EXPERIMENT_PROCESS_ID:-}" = "" ]; then
  echo "ABORTED_E8: missing exclusive identity" >&2
  exit 2
fi
ps -p "$EXPERIMENT_PID" -o pid=,command=
kill -TERM "$EXPERIMENT_PID"
# Esperar 10 s, revalidar PID + identidad y usar SIGKILL solo sobre ese PID.
```

## 7. Orden progresivo y autorizaciones

| Fase | Precondición | Autorización |
|---|---|---|
| E1 | Baseline de solo lectura y schemas verificados | Operador del plan |
| E2 | E1 evaluado y sin fallos de contrato | Aprobación explícita posterior a E1 |
| E3 | Política `reject` aislada; no compartir proceso con E2 | Aprobación independiente |
| E4 | Modelo `READY` y processo identity-verified | Aprobación de residencia/reuso |
| E5 | `time_alive` aislado; sin inferencias ajenas activas | Aprobación de cleanup |
| E6 | `CFG-WAIT` aislada y deadline conocido | Aprobación de timeout |
| E7 | Dos consultas read-only, sin activación adicional | Operador del plan |
| E8 | Utilidad de presión aprobada, umbrales y presupuesto escritos | Aprobación independiente de seguridad |
| E9 | UUID nuevo, lease aislada, proceso LARM dedicado | Operador del plan |

Reglas globales:

- E1 es siempre la primera prueba.
- E2 solo se realiza después de revisar los resultados de E1.
- E3 y todo escenario de presión requieren aprobación independiente.
- Ninguna prueba cierra aplicaciones del usuario, llama a `pkill`, `killall`,
  `osascript` para cerrar apps, ni envía señales por nombre de proceso.
- Ninguna prueba inicia o detiene LARM, `llama-server` ni llama a los endpoints
  administrativos de modelos.
- No reutilizar el mismo proceso LARM entre políticas `wait` y `reject`.

## 8. Criterios globales de PASS/FAIL

Un escenario es PASS únicamente si:

1. todos los comandos usaron el esquema y los endpoints documentados;
2. los IDs, rutas GGUF y configuración correspondieron al registro de la fase;
3. no hubo violación de seguridad ni limpieza incompleta;
4. las evidencias distinguishen `FIT`, autorización, activación, terminación y
   consumo observado;
5. el informe final no extrapola de una sola ejecución.

Es FAIL si hay una violación de contrato o seguridad. Es BLOCKED/ABORTED si una
precondición, un umbral, una aprobación o una limpieza no permite continuar.
No convertir `BLOCKED` en PASS.

## 9. Aborto, fallos parciales y protocolo de limpieza

### 9.1 Abortos

- Timeout de cualquier comando: máximo 15 s para APIs read-only y 30 s para
  crear/liberar una lease. Registrar código HTTP, cuerpo truncado y momento.
- Respuesta ambigua de POST: consultar la lease con la misma clave
  idempotente; nunca repetir con una clave nueva para “ver si carga”.
- Pérdida de LARM o API no disponible: no inferir estado; marcar
  `ABORTED_API_UNAVAILABLE` y escalar a Recovery.
- Presión de E8: aplicar umbrales de la sección 6 y terminar el auxiliar.
- Métricas o eventos inconsistentes: conservar las respuestas crudas y marcar
  `FAIL_OBSERVABILITY`; no fabricar una decisión.

### 9.2 Limpieza

1. Registrar el `lease_id`, `process_instance_id`, PID, modelo y comando de
   proceso **antes** de cualquier acción.
2. Liberar mediante `DELETE /api/v1/leases/{id}` la lease creada por el
   experimento. No eliminar leases ajenas.
3. Esperar y observar `queue`, `status`, `models` y `processes`. No asumir que
   la descarga terminó con la liberación.
4. Si la configuración de la fase permite `keep_alive`/`time_alive`, no exigir
   terminación inmediata. Si `time_alive=0s` y `keep_alive=false`, esperar la
   transición asíncrona; un proceso aún vivo en `STOPPING` no es un fallo
   inmediato, pero sí lo es si nunca termina dentro del timeout de cleanup.
5. Nunca detener procesos del usuario. La única excepción autorizada es el
   proceso auxiliar exclusivo de E8 y solo con identidad verificada.
6. Si queda un proceso experimental vivo, no reutilizar puertos, modelos ni el
   proceso LARM para otra fase. Registrar el PID y `process_instance_id` para
   cleanup manual auditado.
7. Al finalizar, guardar: YAML efectivo, hashes, respuestas HTTP, snapshots,
   eventos, métricas, estados de lease/proceso y bitácora de limpieza.

## 10. Plantilla de informe por escenario

```text
Escenario:
Hora y operador:
Config hash / fingerprint:
Modelo ID / GGUF path / GGUF hash:
PID LARM / ProcessInstanceID:
Lease ID / idempotency key:
Snapshot status / available / swap:
Verdict FIT/INSUFFICIENT/UNKNOWN:
Estado lease / queue_position:
Estado proceso / identity / ownership:
Métricas before / after:
Eventos correlacionados:
Memoria estimada / consumo observado (separados):
Abortos o umbrales:
Cleanup ejecutado y verificado:
PASS / FAIL / BLOCKED:
Observaciones sin extrapolación:
```
