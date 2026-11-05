# Local AI Runtime Manager (LARM)

LARM es un servicio Go independiente de clientes e IDEs que registra modelos GGUF, administra procesos locales de `llama-server` bajo demanda y ofrece un proxy de inferencia compatible con los endpoints esenciales de OpenAI.

## Arquitectura

- `internal/config`: carga YAML, expansión de variables y validación de rutas, puertos y parámetros.
- `internal/models`: registro inmutable de modelos configurados.
- `internal/runtime`: supervisa cada proceso/grupo de procesos y conserva su identidad.
- `internal/scheduler`: worker único, cola FIFO global, leases y políticas de residencia.
- `internal/api/proxy.go`: proxy OpenAI para chat, SSE y embeddings; delega carga/descarga al scheduler.
- `internal/backend`: argumentos de `llama-server` y sondeo `/health`.
- `internal/api`: API HTTP con `net/http`.
- `cmd/larm`: carga configuración, escucha y apagado ordenado.

La API solo acepta direcciones IP loopback (`127.0.0.1` o `::1`); una dirección LAN se rechaza al validar la configuración.

## Requisitos

- Go 1.23 o posterior.
- `llama-server` compilado con soporte para los flags configurados.
- Archivos de modelos GGUF accesibles.

## Compilar y probar

```bash
make build
make test
make test-race
make vet
```

Las pruebas de ciclo de vida usan un proceso HTTP simulado, no descargan ni requieren GGUF reales.

## Configuración y ejecución

```bash
cp configs/models.example.yaml configs/models.yaml
# Edita binary y las rutas de los modelos en configs/models.yaml
make run
```

También se puede indicar el archivo con `./bin/larm -config /ruta/models.yaml` o `LARM_CONFIG`. Se admiten `LARM_HOST`, `LARM_PORT` y `LARM_BINARY` como overrides. Los valores YAML pueden referenciar variables de entorno con `${NOMBRE}` (por ejemplo, `${MODEL_PATH}`). Los valores de ejemplo de `configs/models.example.yaml` reflejan perfiles de `ai-serve`; las rutas son marcadores y se deben sustituir por las locales.

Ejemplo mínimo:

```yaml
server:
  host: "127.0.0.1"
  port: 8090
runtime:
  backend: "llamacpp"
  binary: "/usr/local/bin/llama-server"
  startup_timeout: "120s"
  shutdown_timeout: "15s"
models:
  code-fast:
    path: "/models/code.gguf"
    port: 18069
    context: 16384
    batch: 1024
    ubatch: 256
    gpu_layers: "all"
    cache_type_k: "q8_0"
    cache_type_v: "q8_0"
    parallel: 1
    temperature: 0.2
    top_p: 0.85
    top_k: 40
    reasoning: "off"
    extra_args: ["--jinja"]
    keep_alive: false
    time_alive: "5m"
scheduler:
  max_active_models: 1
  max_queue_size: 100
  lease_ttl: "10m"
  heartbeat_interval: "30s"
  max_lease_duration: "2h"
  # Opcional; por defecto se usa <ruta-config>.idempotency.json
  # idempotency_store: "/var/lib/larm/leases.json"
```

Al iniciar se validan el binario ejecutable, los archivos, los IDs, puertos y parámetros. Cada modelo necesita un puerto propio, distinto del puerto de la API.

## API

```bash
curl http://127.0.0.1:8090/health
curl http://127.0.0.1:8090/api/v1/models
curl http://127.0.0.1:8090/api/v1/status
curl http://127.0.0.1:8090/api/v1/processes
curl -X POST http://127.0.0.1:8090/api/v1/models/code-fast/start
curl -X POST http://127.0.0.1:8090/api/v1/models/code-fast/stop
```

Las operaciones de control esperan a que el backend esté listo o a que termine el apagado. Errores de modelo inexistente, conflicto y backend no disponible responden con `404`, `409` y `503`, respectivamente.

`GET /api/v1/processes` y `GET /api/v1/processes/{process_instance_id}` son consultas read-only de identidad, ownership, grupo, evidencia y terminación. LARM no considera `SIGTERM`, `SIGKILL`, `Wait()` ni un puerto cerrado como prueba de terminación: si el grupo o descendientes no pueden verificarse, conserva `TERMINATION_UNKNOWN` y bloquea el cambio de modelo.

### Scheduler y reservas

Un único worker ejecuta todas las transiciones automáticas del runtime. Las solicitudes se agregan al final de una cola FIFO global; las solicitudes en espera no dependen de que siga abierta la conexión HTTP que las creó. Se admite como máximo una lease activa y un modelo residente. Si el siguiente lease pide el modelo ya cargado, reutiliza el proceso; en caso contrario, LARM confirma su detención antes de iniciar el siguiente.

El ciclo de una reserva es `QUEUED → PREPARING → ACTIVE → COMPLETED` (liberación), o `FAILED`, `CANCELLED` o `EXPIRED`. El TTL empieza al activarse. El heartbeat lo extiende por `lease_ttl` sin superar `max_lease_duration`, que se cuenta desde la activación. La descarga y el siguiente cambio de modelo esperan hasta que terminen las inferencias admitidas, incluso si la lease se libera o vence durante ellas. La cola está solo en memoria y se pierde al reiniciar.

```bash
# Solicitar; HTTP 202 y un lease_id para consultar aunque el modelo siga cargando
curl -X POST http://127.0.0.1:8090/api/v1/leases \
  -H 'Content-Type: application/json' -d '{"model":"code-agent"}'

# Opcional: reintentar el mismo POST de forma idempotente
curl -X POST http://127.0.0.1:8090/api/v1/leases \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 2ef8d7ef-b335-4404-b72c-5b2c08a36101' \
  -d '{"model":"code-agent"}'

curl http://127.0.0.1:8090/api/v1/leases/LEASE_ID
curl -X POST http://127.0.0.1:8090/api/v1/leases/LEASE_ID/heartbeat
curl -X DELETE http://127.0.0.1:8090/api/v1/leases/LEASE_ID
curl http://127.0.0.1:8090/api/v1/queue
curl http://127.0.0.1:8090/api/v1/status
```

`GET /api/v1/leases/{id}` incluye `queue_position` (1-based; cero si no está encolada). `GET /api/v1/queue` y `/status` informan la reserva activa, la que está preparando, la cola pendiente y los modelos residentes con su política, PID y `ProcessInstanceID`. Una cola llena responde `429`; IDs/modelos desconocidos, `404`; heartbeat no aplicable o lease expirado, `409`.

`POST /api/v1/leases` acepta opcionalmente `Idempotency-Key`, en UUID textual `8-4-4-4-12` hexadecimal (no vacío; mayúsculas se normalizan). Sin el header conserva el comportamiento previo. Una clave nueva crea un registro durable antes de añadir una sola lease a FIFO. Un retry con la misma solicitud devuelve la lease/posición original (`202` en `QUEUED`/`PREPARING`, `200` en `ACTIVE`); no renueva TTL ni heartbeat. La única solicitud actual es `model`: su huella es SHA-256 de JSON compacto canónico `{"model":<valor JSON del model decodificado>}`; el valor de modelo es exacto y sensible a mayúsculas. La misma clave con otro modelo devuelve `409`.

Una clave inválida devuelve `400`; un retry de una operación terminal (`COMPLETED`, `EXPIRED`, `CANCELLED` o `FAILED`) devuelve `409` con el estado y snapshot original y nunca recrea la lease. El store registra actividad sucia antes de admitir adquisiciones/arranques y marca apagado limpio solo después de detener el runtime. Si LARM reinicia con estado sucio o una operación no terminal, no restaura ownership ni relanza `llama-server`: conserva IDs históricos y responde `503` (`RECOVERY_REQUIRED`). Mientras exista esa recuperación, bloquea nuevas adquisiciones (con y sin clave) y arranques manuales para no cargar un runtime con ownership desconocido. Requiere intervención operativa; no se adopta ni detiene automáticamente el proceso que sobreviviera a la caída. Un reinicio limpio conserva tombstones terminales y permite claves nuevas. El archivo se guarda atómicamente con permisos `0600`; por defecto está junto al archivo YAML como `<ruta-config>.idempotency.json`, o se configura con `scheduler.idempotency_store`. Las claves/tombstones se retienen indefinidamente (sin limpieza automática); eliminar el archivo reinicia el ámbito de idempotencia, por lo que se debe hacer solo con LARM detenido, tras resolver procesos huérfanos y sin reutilizar claves anteriores. Un archivo no debe compartirse entre procesos LARM independientes.

Los endpoints manuales de LARM-001 se conservan y usan el mismo worker. Solo se aceptan cuando no hay leases activas, solicitudes en preparación ni cola pendiente; de otro modo responden `409`. Así no pueden adelantarse a la cola ni detener un modelo reservado. Una operación manual ya aceptada se procesa antes de leases que lleguen después.

La configuración `scheduler` admite `max_active_models` (positivo), `max_queue_size` (por defecto `100`), `lease_ttl` (`10m`), `heartbeat_interval` (`30s`) y `max_lease_duration` (`2h`). Los tres tiempos deben ser positivos; heartbeat debe ser menor que TTL y la duración máxima al menos igual al TTL.

Cada modelo admite `keep_alive` (por defecto `false`) y `time_alive` (por defecto `"0s"`). `keep_alive: true` mantiene el proceso cargado después de liberar la lease; no precarga el modelo y tiene precedencia sobre `time_alive`. Con `keep_alive: false`, `time_alive` retiene el proceso sólo durante ese período de inactividad. Las duraciones deben ser válidas para `time.Duration` y no negativas. Lease, residencia y Recovery son responsabilidades independientes.

Un fallo de arranque marca la lease `FAILED` y el worker continúa. Una caída inesperada también falla la lease activa; el siguiente trabajo se procesa sin adoptar procesos externos. Al apagar LARM, las leases activas y pendientes se marcan `CANCELLED`, se cancelan cargas e inferencias en curso y se detiene el backend después de que las referencias de inferencia finalicen.

### Proxy de inferencia OpenAI

El proxy expone únicamente `GET /v1/models`, `POST /v1/chat/completions` y `POST /v1/embeddings`. No permite elegir URL/puerto del backend: el puerto se resuelve desde el registro de modelos configurados y las solicitudes pasan por el scheduler FIFO.

En modo automático (sin header de lease), cada inferencia crea una lease, espera su activación y la libera al finalizar. El modelo permanece protegido durante toda la llamada, incluida la transmisión SSE. Ejemplos:

```bash
curl http://127.0.0.1:8090/v1/models

curl http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","messages":[{"role":"user","content":"Explica qué es un mutex en Go"}]}'

curl -N http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","stream":true,"messages":[{"role":"user","content":"Hola"}]}'

curl http://127.0.0.1:8090/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","input":"texto para vectorizar"}'
```

Para reutilizar una lease externa, solicitarla con `POST /api/v1/leases`, esperar a que su estado sea `ACTIVE` y enviar su ID en `X-LARM-Lease-ID` en cada llamada. Por ejemplo:

```bash
curl http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-LARM-Lease-ID: LEASE_ID' \
  -d '{"model":"code-agent","messages":[{"role":"user","content":"Siguiente pregunta"}]}'
```

La lease debe existir, estar activa y corresponder al modelo del JSON. El proxy no la libera. Si vence durante una inferencia, no admite llamadas nuevas, deja terminar la ya admitida y después libera el runtime. El ID no es una credencial: esta modalidad solo es segura para clientes locales confiables y no proporciona autorización multiusuario.

`proxy.queue_wait_timeout` (por defecto `5m`) solo limita la espera FIFO; `inference_timeout` (`10m`) limita cada inferencia; `backend_connect_timeout` (`5s`) limita la conexión al backend, y `stream_idle_timeout` (`2m`) limita el tiempo sin fragmentos SSE. Desconectar/cancelar una petición en cola elimina su lease automática; durante inferencia cancela el HTTP al backend y libera la referencia cuando este termina. El apagado cancela inferencias admitidas y espera su cierre antes de detener el proceso.

`GET /api/v1/status` conserva el estado de runtime y scheduler y añade `queued_requests`, `active_inferences`, `completed_requests` y `failed_requests`. No registra ni conserva prompts o respuestas.

El proxy reenvía las respuestas JSON y los eventos SSE de `llama-server`, incluidos campos presentes como `usage`, `finish_reason` y `tool_calls`; no implementa adaptaciones específicas por modelo.

## Estados

`STOPPED → STARTING → READY`, con `FAILED` ante un fallo de inicio o salida inesperada; `READY → STOPPING → STOPPED` al detener. `max_active_models` limita estrictamente los procesos residentes. Un `Start` repetido para el mismo modelo listo es idempotente.

## Limitaciones actuales

- Solo implementa los tres endpoints listados; no incluye Responses API, audio, imágenes ni otros endpoints OpenAI.
- No incluye prioridades, persistencia de cola, gestor de memoria RAM/VRAM ni autenticación multiusuario.
- Solo administra procesos `llama-server` iniciados por LARM; no adopta procesos externos.
- No valida que el modelo pueda cargarse en la memoria/GPU disponible; readiness lo determina el endpoint `/health`.
- Los argumentos avanzados dependen de la versión/compilación de `llama-server` instalada.
