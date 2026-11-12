# LARM-007.2.1 — Memory Capacity Planner (Dry-Run)

## Resultado

El planificador expone una consulta reproducible y explicable de capacidad sin
participar en admisión, Scheduler, leases, residencia, ciclo de vida, Recovery
ni liberación de modelos.

## Arquitectura y decisiones

- `internal/capacity` recibe una función que devuelve un `memory.Snapshot` y
  otra que devuelve la configuración de un modelo. No conoce ni depende de
  `memory.Observer`; el runtime entrega una copia read-only del snapshot.
- Lee el tamaño del archivo y metadata GGUF (capas, embedding, cabezas KV,
  longitudes K/V). Estima `archivo + KV-cache + workspace de batch`.
- El KV-cache usa `context * parallel * layers * kv_heads * (key_length +
  value_length)` y los tipos de cache configurados. El workspace usa dos
  buffers fp16 como aproximación conservadora. Esto es una estimación, no una
  reproducción del allocator de llama.cpp.
- En `architecture: unified` compara la demanda total con RAM disponible. En
  Linux/NVIDIA separa RAM y VRAM según `gpu_layers`; suma sólo `free_bytes`
  entregados por el proveedor. Si el proveedor falta, es parcial o no entrega
  VRAM, el resultado es `UNKNOWN`, nunca una métrica inventada.
- Nunca suma RSS ni private bytes de procesos. `available_bytes`/`free_bytes`
  son la evidencia de capacidad actual y pueden ya incluir presión global.
- El resultado incluye el snapshot completo, estados, fuentes y timestamps, más
  componentes de la estimación, márgenes, capacidad observada, justificación y
  limitaciones.

## Márgenes

Los valores iniciales documentados son `10%` para RAM y `10%` para GPU:

```yaml
capacity:
  system_margin_percent: 10
  gpu_margin_percent: 10
```

Se valida que cada margen sea `>= 0` y `< 100`. El margen se aplica por dominio
después de separar RAM/VRAM. Es una configuración del análisis; no es un límite
de seguridad para ejecución.

## Contrato de resultados

Cada consulta devuelve `FIT`, `INSUFFICIENT` o `UNKNOWN`. Snapshots `STALE`,
`UNAVAILABLE`, componentes sin bytes, metadata GGUF incompleta y proveedores
GPU ausentes producen `UNKNOWN` cuando impiden una decisión reproducible.

```bash
curl http://127.0.0.1:8090/api/v1/capacity/code-fast
larm capacity code-fast
```

También se admite `GET /api/v1/capacity?model=code-fast`. Ambos son GET-only;
un modelo inexistente produce `UNKNOWN` con justificación, no una mutación.

Ejemplo de salida CLI/API obtenido con el fixture GGUF de las pruebas (la
salida real además incluye el snapshot completo):

```json
{
  "verdict": "FIT",
  "model_id": "model",
  "estimate": {
    "file_bytes": 291,
    "context_tokens": 128,
    "batch_tokens": 8,
    "kv_cache_bytes": 1048576,
    "raw_bytes": 1065251,
    "required_bytes": 1065251
  },
  "justification": ["la demanda estimada cabe en available_bytes; no se sumaron RSS de procesos"]
}
```

Este ejemplo es reproducible con el fixture de pruebas; no es una garantía de
consumo de hardware real. La respuesta de la API siempre contiene además el
snapshot, sus fuentes/fechas y las limitaciones.

## Matriz CAP-01–CAP-27

| ID | Caso | Resultado/evidencia |
|---|---|---|
| CAP-01 | Memoria suficiente | PASS — `TestCAP0103CapacityVerdicts`, `FIT` |
| CAP-02 | Memoria insuficiente | PASS — misma prueba, `INSUFFICIENT` |
| CAP-03 | Memoria desconocida | PASS — misma prueba, `UNKNOWN` |
| CAP-04 | Snapshot AVAILABLE | PASS — `TestCAP0406SnapshotStates` |
| CAP-05 | Snapshot STALE | PASS — `UNKNOWN` |
| CAP-06 | Snapshot UNAVAILABLE | PASS — `UNKNOWN` |
| CAP-07 | Apple Silicon | PASS — `TestCAP0709UnifiedMemoryDoesNotNeedGPUProvider` |
| CAP-08 | Memoria unificada | PASS — presupuesto único de sistema |
| CAP-09 | Sin VRAM inventada | PASS — proveedor Apple permanece `UNAVAILABLE` |
| CAP-10 | NVIDIA fixture | PASS — `TestCAP1012NVIDIAFixtureAndAbsentProvider` (fixture) |
| CAP-11 | NVIDIA separado RAM/VRAM | PASS — comparación por dominios (fixture) |
| CAP-12 | NVIDIA sin proveedor | PASS — `UNKNOWN`; no es validación hardware real |
| CAP-13 | Archivo GGUF | PASS — metadata y tamaño se conservan |
| CAP-14 | Contexto | PASS — KV-cache crece con `context` |
| CAP-15 | Batch/UBatch/parámetros | PASS — batch y configuración aparecen en `Estimate` |
| CAP-16 | Margen RAM | PASS — margen configurable por dominio |
| CAP-17 | Margen GPU | PASS — margen configurable por dominio |
| CAP-18 | Límites | PASS — `Options.Validate` rechaza `100`, negativos, NaN e Inf |
| CAP-19 | CLI | PASS — `capacity MODEL`, GET-only |
| CAP-20 | API path | PASS — `GET /api/v1/capacity/{model}` |
| CAP-21 | API query | PASS — `GET /api/v1/capacity?model=...` |
| CAP-22 | Concurrencia | PASS — 32 consultas concurrentes |
| CAP-23 | Race detector | PASS — `go test -race ./... -count=1` |
| CAP-24 | Datos inmutables | PASS — planner sólo consume copias/valores |
| CAP-25 | Scheduler | PASS — no hay llamadas al Scheduler desde `internal/capacity` |
| CAP-26 | Runtime | PASS — sólo se añadieron getters read-only |
| CAP-27 | Recovery | PASS — constructor y endpoints de Recovery no se modifican semánticamente |

CAP-10–12 usan fixtures, no constituyen una validación NVIDIA real.

## Limitaciones, riesgos y pendientes

- El formato GGUF no describe por sí solo el allocator, buffers temporales,
  gráficos backend, mmap, fragmentación, offload exacto o concurrencia real de
  llama.cpp. La fórmula es deliberadamente conservadora pero no exacta.
- La división de capas es proporcional para `gpu_layers`; multi-GPU usa la suma
  de VRAM libre reportada y no modela topología ni restricciones de una GPU
  individual.
- No se descarga, inicia, detiene ni bloquea nada. Una futura política de
  admisión requeriría otro componente y otra decisión explícita.
- Falta una validación de hardware NVIDIA real y calibración con llama.cpp
  ejecutándose en hosts representativos.
