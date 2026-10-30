# Local AI Runtime Manager (LARM)

LARM es un servicio Go independiente de clientes e IDEs que registra modelos GGUF y administra un único proceso local de `llama-server`. Esta entrega cubre el registro, el ciclo de vida del proceso y el estado; no incluye inferencia proxy.

## Arquitectura

- `internal/config`: carga YAML, expansión de variables y validación de rutas, puertos y parámetros.
- `internal/models`: registro inmutable de modelos configurados.
- `internal/runtime`: serializa operaciones y supervisa el proceso/grupo de procesos.
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
```

Al iniciar se validan el binario ejecutable, los archivos, los IDs, puertos y parámetros. Cada modelo necesita un puerto propio, distinto del puerto de la API.

## API

```bash
curl http://127.0.0.1:8090/health
curl http://127.0.0.1:8090/api/v1/models
curl http://127.0.0.1:8090/api/v1/status
curl -X POST http://127.0.0.1:8090/api/v1/models/code-fast/start
curl -X POST http://127.0.0.1:8090/api/v1/models/code-fast/stop
```

Las operaciones de control esperan a que el backend esté listo o a que termine el apagado. Errores de modelo inexistente, conflicto y backend no disponible responden con `404`, `409` y `503`, respectivamente.

## Estados

`STOPPED → STARTING → READY`, con `FAILED` ante un fallo de inicio o salida inesperada; `READY → STOPPING → STOPPED` al detener. Se permite un único modelo activo. Un `Start` repetido para el mismo modelo listo es idempotente.

## Limitaciones de LARM-001

- No incluye cola, proxy de inferencia/OpenAI, sesiones, leases, persistencia ni selección/prioridades.
- Solo administra procesos `llama-server` iniciados por LARM; no adopta procesos externos.
- No valida que el modelo pueda cargarse en la memoria/GPU disponible; readiness lo determina el endpoint `/health`.
- Los argumentos avanzados dependen de la versión/compilación de `llama-server` instalada.
