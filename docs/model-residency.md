# Residencia de modelos

La `lease` autoriza el uso de un modelo para una solicitud; no determina por sí
sola si `llama-server` permanece cargado. La residencia la decide la política
del modelo y Recovery conserva autoridad independiente sobre estados ambiguos.

```yaml
scheduler:
  max_active_models: 3
models:
  commit:
    path: /models/commit.gguf
    port: 8069
    keep_alive: true
    time_alive: "0s"
  code-fast:
    path: /models/code-fast.gguf
    port: 8070
    keep_alive: false
    time_alive: "10m"
```

Los valores por defecto son `keep_alive: false` y `time_alive: "0s"`, que
conservan el comportamiento bajo demanda. `keep_alive` prevalece sobre
`time_alive`; las duraciones inválidas o negativas se rechazan.

- `keep_alive: true`: LARM carga el modelo automáticamente en segundo plano
  después de Recovery y la inicialización del Scheduler. No crea leases ni
  inferencias; una liberación de lease no detiene el proceso y las solicitudes
  siguientes reutilizan su `ProcessInstanceID`. Un stop administrativo suprime
  la precarga automática hasta una nueva solicitud válida o un nuevo arranque.
- `time_alive > 0`: LARM también carga el modelo automáticamente. Cuando queda
  READY sin leases, inferencias ni protecciones, comienza la retención. Una
  solicitud antes del vencimiento reutiliza el proceso y reinicia el período;
  el vencimiento detiene el proceso de forma verificada y no vuelve a
  precargarlo automáticamente.
- `time_alive: "0s"`: se descarga al quedar ocioso, como antes.

La precarga se ordena de forma determinista por ID de modelo y se ejecuta en el
worker del Scheduler, después de que el gate de Recovery queda instalado. Las
solicitudes ya admitidas conservan FIFO. Si la capacidad no permite una
precarga, el estado observable registra `SKIPPED_CAPACITY` y el modelo queda
disponible para adquisición normal; no se mantiene un reintento en bucle. Los
estados `QUEUED`, `STARTING`, `READY`, `FAILED`, `BLOCKED_RECOVERY` y
`CANCELLED` aparecen en `preload_state` mediante los endpoints y la CLI
existentes.

`max_active_models` es estricto incluso durante cargas concurrentes. Un modelo
persistente nunca se desaloja automáticamente; la solicitud que no puede
progresar termina con un error explícito de capacidad. Un modelo temporal ocioso
puede desalojarse anticipadamente, respetando FIFO, leases, gates y Runtime
Manager. No se usa force release, heartbeat ficticio ni TTL de lease como
temporizador de residencia.

Cuando la causa verificable es una plaza ocupada por residentes, la inferencia
conserva el contrato HTTP `409` y devuelve un diagnóstico específico, por
ejemplo: `model capacity exhausted: 1/1 resident slots occupied`.

`GET /api/v1/status`, `GET /api/v1/queue` y `GET /api/v1/models` conservan la
información operativa de residentes: política efectiva, estado, PID y
`ProcessInstanceID`. La respuesta no privilegiada de `/api/v1/models` omite la
ruta absoluta `path`; la configuración interna y Runtime conservan esa ruta
para cargar el GGUF. Recovery continúa fail-closed: un crash no adopta
procesos supervivientes y `keep_alive` no es evidencia de ownership.

## Control administrativo explícito

La consulta usa esos contratos existentes. Las operaciones mutantes no están
disponibles por HTTP público ni por el proxy OpenAI. El control privilegiado usa
el Unix socket administrativo, identidad de peer y una capability Ed25519 con
audience, expiración, permiso `model.stop`, modelo objetivo y protección contra
replay. El cliente puede emitirla y ejecutar:

```text
larm admin -socket ... -private-key ... -out ... auth issue-model-stop <model-id>
larm admin -socket ... -capability-file ... model stop <model-id>
```

El Scheduler serializa el stop con adquisiciones y timers. Rechaza Recovery
bloqueado, leases activas o inferencias protegidas; Runtime Manager sólo libera
capacidad después de probar identidad, ausencia del proceso y de sus
descendientes. Un timeout conserva `UNKNOWN`/Recovery Required y nunca se
reporta como descarga exitosa. `keep_alive` y `time_alive` permanecen en la
configuración: un stop explícito sólo descarga la instancia actual y una nueva
solicitud válida puede cargarla de nuevo.
