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
conservan el comportamiento anterior. No hay precarga. `keep_alive` prevalece
sobre `time_alive`; las duraciones inválidas o negativas se rechazan.

- `keep_alive: true`: primera solicitud carga el modelo; una liberación de
  lease no detiene el proceso y las solicitudes siguientes reutilizan su
  `ProcessInstanceID`.
- `time_alive > 0`: después de quedar sin leases, inferencias ni protecciones,
  comienza la retención. Una solicitud antes del vencimiento reutiliza el
  proceso y reinicia el período; el vencimiento detiene el proceso de forma
  verificada.
- `time_alive: "0s"`: se descarga al quedar ocioso, como antes.

`max_active_models` es estricto incluso durante cargas concurrentes. Un modelo
persistente nunca se desaloja automáticamente; la solicitud que no puede
progresar termina con un error explícito de capacidad. Un modelo temporal ocioso
puede desalojarse anticipadamente, respetando FIFO, leases, gates y Runtime
Manager. No se usa force release, heartbeat ficticio ni TTL de lease como
temporizador de residencia.

`GET /api/v1/status`, `GET /api/v1/queue` y `GET /api/v1/models` conservan sus
contratos y añaden información de residentes: política efectiva, estado, PID y
`ProcessInstanceID`. Recovery continúa fail-closed: un crash no adopta procesos
supervivientes y `keep_alive` no es evidencia de ownership.
