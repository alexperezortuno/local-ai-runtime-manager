# LARM-009 — Parámetros opcionales de llama.cpp

## Alcance

LARM-009 añade parámetros opcionales por modelo para `llama-server`. La
configuración anterior sigue siendo válida. Si un campo no aparece en YAML,
LARM no genera su flag y deja que `llama-server` use su valor predeterminado.

La inspección obligatoria se realizó sobre:

```text
llama-server 0.4.0 (build 10809, commit 5266f24da)
```

El binario instalado expone `--n-predict` (también `--predict`), `--flash-attn`,
`--gpu-layers/--n-gpu-layers`, `--device`, `--split-mode`, `--fit`,
`--cache-prompt`, `--metrics` y `--no-mmproj`. No expone `--no-nmproj`; por eso
`no_nmproj` es el nombre YAML de LARM y se traduce al flag real
`--no-mmproj`.

## Parámetros

| YAML | Descripción | Tipo | Valores permitidos | Default backend | Equivalencia llama.cpp | Compatibilidad 0.4.0 | Ejemplo | Observaciones |
|---|---|---|---|---|---|---|---|---|
| `presence_penalty` | Penalización por presencia de un token | `float` opcional | Número finito; `0` deshabilita | `0.00` | `--presence-penalty N` | Sí | `presence_penalty: 0.2` | Es un default de arranque; una petición explícita puede reemplazarlo. |
| `frequency_penalty` | Penalización por frecuencia de un token | `float` opcional | Número finito; `0` deshabilita | `0.00` | `--frequency-penalty N` | Sí | `frequency_penalty: 0.1` | No se limita artificialmente a `[0,1]`; el binario no declara ese rango. |
| `n_predict` | Máximo predeterminado de tokens a predecir | `int` opcional | `-1` o entero `>= 0` | `-1` (infinito) | `--n-predict N` | Sí | `n_predict: 512` | No modifica `max_tokens`/parámetros enviados por OpenCode o Codex. |
| `repeat_penalty` | Penalización de secuencias repetidas | `float` opcional | Finito `>= 0`; `1` deshabilita | `1.00` | `--repeat-penalty N` | Sí | `repeat_penalty: 1.1` | Default de servidor, no reescribe solicitudes. |
| `flash_attn` | Control de Flash Attention | `string` opcional | `on`, `off`, `auto` | `auto` | `--flash-attn on\|off\|auto` | Sí | `flash_attn: auto` | `auto` deja decidir al backend. |
| `device` | Dispositivos usados para offload | `string` opcional | `none` o lista separada por comas | No indicado por `--help`; decide el backend | `--device DEV1,DEV2` | Sí | `device: "Metal"` | Los nombres deben obtenerse con `llama-server --list-devices`; no se validan físicamente. |
| `gpu_layers` | Capas máximas en VRAM, campo histórico | `string` requerido por la configuración anterior | `all` o entero `>= 0` | `auto` en el backend, pero LARM conserva su contrato histórico | `-ngl N` | Sí, histórico | `gpu_layers: "all"` | Se conserva sin cambios para compatibilidad. |
| `n_gpu_layers` | Alias tipado de `gpu_layers` | `string` opcional | Exactamente los mismos valores que `gpu_layers` | `auto` en el backend si no se especifica | `-ngl N` mediante `--n-gpu-layers` como alias | Sí | `n_gpu_layers: "all"` | Si ambos aparecen deben ser idénticos; LARM genera un solo flag. |
| `split_mode` | Estrategia de reparto entre GPUs | `string` opcional | `none`, `layer`, `row`, `tensor` | `layer` | `--split-mode MODE` | Sí | `split_mode: layer` | `tensor` es experimental según el binario. |
| `metrics` | Habilita endpoint Prometheus | `bool` opcional | `true`, `false` | `false` | `true` → `--metrics`; `false` → sin flag | Sí | `metrics: true` | El binario no expone `--no-metrics`; `false` conserva el default deshabilitado. |
| `fit` | Ajusta argumentos no fijados para caber en memoria | `bool` opcional | `true`, `false` | `on` | `true` → `--fit on`; `false` → `--fit off` | Sí | `fit: false` | Puede interactuar con `context`/memoria; no cambia Capacity Admission. |
| `cache_prompt` | Habilita cacheado de prompts | `bool` opcional | `true`, `false` | `enabled` | `true` → `--cache-prompt`; `false` → `--no-cache-prompt` | Sí | `cache_prompt: true` | El cache reuse requiere prompt caching en llama.cpp. |
| `no_nmproj` | Desactiva el uso automático de multimodal projector | `bool` opcional | `true`, `false` | Projector habilitado | `true` → `--no-mmproj`; `false` → `--mmproj-auto` | Sí, mediante flag equivalente | `no_nmproj: true` | `no_nmproj` no es un flag de llama.cpp; es el nombre YAML solicitado. Sólo importa para modelos/procedencia multimodal. |

Los defaults de la tabla son los mostrados por el `--help` del binario
inspeccionado. Cuando el binario no declara un default, la tabla lo indica y
LARM no inventa uno.

## Ejemplo YAML comentado

```yaml
models:
  code-fast:
    path: "/models/code.gguf"
    port: 18069
    context: 16384
    batch: 1024
    ubatch: 256

    # Campo histórico: capas que se intentan colocar en VRAM.
    gpu_layers: "all"
    # Alias exacto de gpu_layers; no usar un valor contradictorio junto al anterior.
    # n_gpu_layers: "all"

    # Penalización por presencia; 0 conserva el default deshabilitado.
    presence_penalty: 0.2
    # Penalización por frecuencia; 0 conserva el default deshabilitado.
    frequency_penalty: 0.1
    # Tokens máximos predeterminados; -1 significa infinito.
    n_predict: 512
    # Penalización de repetición; 1 la deshabilita.
    repeat_penalty: 1.1

    # Flash Attention: on, off o auto.
    flash_attn: auto
    # Lista de dispositivos de offload, o none.
    device: "Metal"
    # Reparto multi-GPU: none, layer, row o tensor.
    split_mode: layer

    # Expone métricas Prometheus cuando es true.
    metrics: true
    # Ajusta parámetros no fijados para caber en memoria.
    fit: false
    # Activa o desactiva el prompt cache.
    cache_prompt: true
    # true usa el flag real --no-mmproj; false deja el projector automático.
    no_nmproj: false

    # Se mantiene para flags que LARM no tipa. No repitas aquí un flag tipado.
    extra_args:
      - "--jinja"
```

## Sampling y solicitudes

`temperature`, `top_p`, `top_k`, `presence_penalty`,
`frequency_penalty`, `repeat_penalty` y `n_predict` son argumentos de
arranque que establecen defaults del servidor. Los punteros usados por LARM
permiten distinguir un campo ausente de `0` explícito; los booleanos permiten
distinguir `false` explícito.

Los sampling históricos se traducen a `--temp`, `--top-p` y `--top-k`. En el
binario inspeccionado sus defaults son respectivamente `0.80`, `0.95` y `40`.

LARM no modifica Scheduler, leases, Recovery, Capacity Admission, seguridad
LAN, el proxy OpenAI ni los cuerpos de solicitud de OpenCode/Codex. Por tanto,
un valor de sampling enviado explícitamente por una solicitud sigue siendo
responsabilidad de la ruta de inferencia y no se sobrescribe con la
configuración de arranque.

## Validación y compatibilidad

- Los errores incluyen el ID del modelo y el parámetro.
- Un flag tipado repetido en `extra_args` se rechaza, incluyendo aliases.
- `gpu_layers` y `n_gpu_layers` contradictorios se rechazan; nunca se generan
  dos flags equivalentes.
- Al usar cualquier parámetro nuevo, el loader ejecuta
  `runtime.binary --help` y rechaza opciones no anunciadas por ese binario.
- Las configuraciones sin parámetros nuevos no ejecutan esa inspección y
  conservan su comportamiento previo.
- La compatibilidad indicada con llama.cpp 0.4.0 está basada en el binario
  instalado e inspeccionado, no en nombres de flags de versiones anteriores.
- No se realizan pruebas físicas ni se inician modelos GGUF.
