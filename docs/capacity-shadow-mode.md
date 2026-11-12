# LARM-007.3.1 — Capacity-Aware Admission (Shadow Mode)

## Arquitectura

El punto de admisión existente sigue siendo `Scheduler.Request` (y la reserva
idempotente). El Scheduler clasifica la observación como `NEW_LOAD`,
`RESIDENT_REUSE` o `QUEUED` y la entrega a `capacity.Shadow` sin esperar su
resultado. Para inferencias automáticas se conserva el `request_id`; para
reservas externas se usa el `lease_id`.

`capacity.Shadow` mantiene una cola y un historial acotado en memoria. Un worker
ejecuta el `Capacity Planner` con timeout y registra sólo un resumen: verdict,
fuentes, memoria disponible/estimada, requerimientos con márgenes y
justificación. No incluye rutas GGUF, prompts, respuestas ni argumentos de
backend. `UNKNOWN`, timeout, métricas `STALE` y errores del planner son
observaciones no bloqueantes.

El historial es histórico y no representa el estado actual de memoria. Se
consulta en `GET /api/v1/capacity/evaluations` o `larm capacity-evaluations`.
El modo se habilita con `capacity.shadow_mode: true`; con `false` no se instala
el observador y el comportamiento normal permanece igual.

## Ejemplos

```bash
curl http://127.0.0.1:8090/api/v1/capacity/evaluations
larm capacity-evaluations
```

```json
[
  {
    "evaluation_id": "…",
    "request_id": "…",
    "model_id": "code-fast",
    "operation": "NEW_LOAD",
    "verdict": "FIT",
    "timestamp": "2026-11-11T12:00:00Z",
    "metrics_source": "host-memory",
    "observation_state": "EVALUATED",
    "system_available_bytes": 17179869184,
    "estimated_bytes": 12000000000,
    "required_bytes": 13200000000,
    "system_margin_percent": 10,
    "gpu_margin_percent": 10,
    "justification": ["la demanda estimada cabe en available_bytes"]
  }
]
```

## Matriz SHADOW-01–SHADOW-15

| ID | Cobertura | Evidencia |
|---|---|---|
| SHADOW-01–03 | FIT, INSUFFICIENT, UNKNOWN | `capacity.Planner` y resumen shadow |
| SHADOW-04 | Reutilización residente | operación `RESIDENT_REUSE` |
| SHADOW-05 | Solicitud en cola | operación `QUEUED` |
| SHADOW-06–07 | Error, timeout y STALE | estado `ERROR`, verdict `UNKNOWN` |
| SHADOW-08 | Concurrencia | historial protegido por mutex y cola asíncrona |
| SHADOW-09 | FIFO | Scheduler no cambia `queue.push/pop` |
| SHADOW-10 | Leases | sólo se añade un callback read-only |
| SHADOW-11 | Recovery | no se escribe journal ni estado de Recovery |
| SHADOW-12 | API | `GET /api/v1/capacity/evaluations` |
| SHADOW-13 | CLI | `larm capacity-evaluations` |
| SHADOW-14 | Race detector | validación `go test -race ./... -count=1` |
| SHADOW-15 | Regresiones | `go test ./... -count=1` |

Las decisiones de Scheduler son idénticas con `shadow_mode` habilitado o
deshabilitado: el resultado del planner nunca se consulta en `Request`,
`step`, `activate`, FIFO, leases, Recovery o ciclo de vida.

## Limitaciones y riesgos

- La evaluación es asíncrona y puede quedar `DROPPED` si se llena su cola; esto
  nunca rechaza ni demora una solicitud.
- El historial no es durable y se pierde al reiniciar.
- El planner conserva las limitaciones de la fórmula GGUF y la calibración
  NVIDIA real sigue pendiente.
