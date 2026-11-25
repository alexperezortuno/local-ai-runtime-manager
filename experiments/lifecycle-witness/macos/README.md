# macOS lifecycle-witness

Experimento aislado para `LARM-006.6.V2.3.2`. Solo usa procesos de prueba
locales; no arranca `llama-server`, no carga modelos y no instala servicios.

Requiere macOS, Go con cgo y un usuario no root:

```sh
./experiments/lifecycle-witness/macos/run.sh
```

El primer proceso (`requester`) crea al `supervisor` y termina después de que
este publica `ready.json`. El supervisor crea un hijo en un grupo de procesos
propio, espera su terminación con `wait4`, y registra el cambio de PPID tras la
salida del requester. El proceso `runtime`, lanzado después desde el shell,
valida PID + identidad de inicio antes de enviar `SIGTERM`.

El supervisor intenta `SIGUSR1` sobre el hijo como prueba segura de autoridad.
El hijo maneja esa señal y sigue vivo. Un resultado exitoso es evidencia de que
macOS no separa la autoridad de señales por proceso cuando ambos usan el mismo
UID; en ese caso TEST-04 es `UNSUPPORTED`, aunque la operación no termine al
hijo.

La salida JSONL se conserva en el directorio temporal indicado por `run.sh`:

- `supervisor.jsonl`: spawn, reparentado, intento no autorizado y `wait4`.
- `child.jsonl`: identidad del hijo, señal recibida y descendiente.
- `runtime.jsonl`: identidad del runtime y resultado de `SIGTERM`.
