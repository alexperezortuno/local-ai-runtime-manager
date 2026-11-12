# Observabilidad de memoria

LARM publica observaciones de memoria en los snapshots existentes de
`GET /api/v1/status` y `GET /api/v1/models`. La observación no participa en
admisión, FIFO, leases, residencia, precarga, Recovery ni decisiones de carga.

## Fuentes y semántica

- Linux usa `/proc/meminfo` para RAM y swap, `/proc/<pid>/status` para RSS y
  `smaps_rollup` para `Private_Clean + Private_Dirty` cuando está disponible.
  También informa `memory.max`/`memory.current` del cgroup visible, sin
  sustituir la RAM física del host.
- macOS usa `hw.memsize`, estadísticas públicas de Mach VM y `vm.swapusage`.
  RSS y la huella física del proceso proceden de `libproc`/`proc_pid_rusage`.
- En Apple Silicon `architecture` es `unified`: RAM y GPU comparten el mismo
  presupuesto físico. LARM no inventa VRAM dedicada ni suma RAM y VRAM.
- Linux intenta consultar `nvidia-smi` con un timeout de 500 ms. Es opcional;
  ausencia, driver inaccesible, permisos, timeout y respuestas parciales son
  estados explícitos. No se atribuye memoria GPU a un proceso sin una fuente
  fiable.

`available_bytes` en Linux es `MemAvailable` cuando existe; es una estimación
del kernel, no memoria libre garantizada. En macOS es una estimación basada en
las páginas libres, inactivas y especulativas. RSS incluye memoria compartida y
no equivale a memoria física exclusiva; por eso LARM no suma RSS de procesos.
La huella física de macOS es un equivalente de presión física, no una promesa
de exclusividad.

Todos los campos de bytes son `null` cuando no se conocen; cero no significa
desconocido. Cada métrica tiene `status`, `source` y `measured_at`. Un snapshot
con más de tres intervalos (intervalo fijo de 2 s) se marca `STALE`. El
observador se cancela durante shutdown y sus errores no interrumpen inferencias.

## Modelo resumido

```json
{
  "memory": {
    "system": {
      "architecture": "unified",
      "total_bytes": 17179869184,
      "available_bytes": 8589934592,
      "used_bytes": 8589934592,
      "swap_total_bytes": null,
      "swap_used_bytes": null,
      "measured_at": "2026-11-09T12:00:00Z",
      "status": "AVAILABLE",
      "source": "macOS:sysctl+Mach VM",
      "cgroup_limit_bytes": null,
      "cgroup_used_bytes": null,
      "cgroup_status": "UNAVAILABLE",
      "cgroup_source": ""
    },
    "gpus": [],
    "gpu_provider": {"status": "UNAVAILABLE", "source": "apple-unified-memory"},
    "processes": [],
    "measured_at": "2026-11-09T12:00:00Z",
    "status": "AVAILABLE",
    "source": "larm-memory-observer"
  }
}
```

Los procesos se asocian simultáneamente por PID, `StartIdentity`,
`ProcessInstanceID` y ownership existente. Un PID reutilizado o una instancia
que ya salió produce `UNAVAILABLE` y no hereda la medición anterior.
