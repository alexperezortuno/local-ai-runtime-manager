# LARM-008.3 — OpenAI Responses API para Codex CLI

Compatibilidad de LARM con la OpenAI Responses API para usar Codex CLI con modelos administrados por llama.cpp.

## Estado

PASS

El contrato de la Responses API está soportado por el proxy de LARM como **proxy transparente**: el cuerpo de la petición y los eventos de la respuesta se reenvían sin transformar. La única adaptación es la detección del terminador de stream, porque la Responses API no usa el centinela `[DONE]` de Chat Completions.

**Limitación conocida (no bloqueante para HTTP):** Codex CLI 0.169.0 envía un item `additional_tools` en el array `input` cuando la feature `incremental_tools` está activa. llama.cpp 0.4.0 rechaza ese tipo de item con HTTP 400 (`Cannot determine type of 'item'`). La mitigación es `--disable incremental_tools`, documentada en la sección 7 y aplicada en los perfiles de referencia.

---

## 1. Contratos inspeccionados antes del cambio

| Fuente | Contrato real verificado |
|---|---|
| Codex CLI instalado | `codex-cli 0.169.0`. `wire_api = "chat"` **dejó de estar soportado**: `CHAT_WIRE_API_REMOVED_ERROR` = "`wire_api = \"chat\"` is no longer supported." El enum `WireApi` solo tiene la variante `Responses`. |
| Codex: ruta de la Responses API | `codex-rs/codex-api/src/lib.rs`: `pub const RESPONSES_PATH: &str = "/responses";`. `Provider::url_for_path` concatena `base_url.trim_end_matches('/')` + `"/"` + `path`. Por tanto `base_url = "http://host:8090/v1"` produce `http://host:8090/v1/responses`. |
| Codex: petición HTTP | `codex-rs/codex-api/src/common.rs`, struct `ResponsesApiRequest`: `model`, `stream`, `service_tier`, `input: Vec<ResponseItem>`, `tools`, `tool_choice`, `parallel_tool_calls`, `reasoning`, `store`, `stream_options`, `include`, `prompt_cache_key`, `prompt_cache_options`, `text`, `client_metadata`, `access_programs`. **No contiene `previous_response_id` ni `instructions`.** |
| Codex: `previous_response_id` | Solo existe en `ResponseCreateWsRequest` (transporte WebSocket). El transporte HTTP reenvía siempre el historial completo en `input`. |
| Codex: parser SSE | `codex-rs/codex-api/src/sse/responses.rs`. Usa `eventsource_stream::Eventsource` y decide por el campo `type` **dentro del JSON de `data:`**, no por el nombre del evento SSE. Tolera eventos desconocidos. Si el stream termina sin `response.completed` emite `ApiError::Stream("stream closed before response.completed")`. |
| Codex: eventos que consume | `response.created`, `response.output_item.added`, `response.output_item.done`, `response.output_text.delta`, `response.completed`, `response.failed`, `response.incomplete`, `response.custom_tool_call_input.delta`, `response.reasoning_summary_text.delta`, `response.reasoning_summary_text.done`, `response.reasoning_text.delta`. |
| Codex: item `FunctionCall` | `codex-rs/protocol/src/models.rs`: `arguments: String` (JSON serializado en texto), `call_id: String`, `name: String`. |
| Codex: item `FunctionCallOutput` | `FunctionCallOutputPayload` serializa `output` como texto plano **o** como array de `FunctionCallOutputContentItem`, cuyo tipo es `input_text` (`{"type":"input_text","text":"..."}`). |
| Codex: perfiles | `codex -p/--profile <name>` superpone `$CODEX_HOME/<name>.config.toml` sobre la configuración base. **No** usa una sección `[profiles.<name>]`. |
| Codex: TLS | `codex-rs/http-client/src/custom_ca.rs`: `CODEX_CA_CERTIFICATE`, con fallback a `SSL_CERT_FILE`. PEM con bloques `CERTIFICATE`. |
| llama-server instalado | `llama-server 0.4.0 (build 10809)`. Rutas registradas: `POST /v1/responses` y `POST /responses` (`tools/server/server.cpp`). |
| llama-server: implementación | `tools/server/server-context.cpp`: `post_responses_oai` llama a `server_chat_convert_responses_to_chatcmpl(json::parse(req.body))` y luego a `handle_completions_impl(..., TASK_RESPONSE_TYPE_OAI_RESP)`. Es decir, llama-server **convierte internamente** Responses → Chat Completions y emite los eventos Responses él mismo. |
| llama-server: conversión de entrada | `tools/server/server-chat.cpp`. Acepta `input` como string o array. Tipos de item soportados: `message` (roles `user`/`system`/`developer`/`assistant`, contenido `input_text`/`input_image`), `function_call`, `function_call_output`, `reasoning`. Cualquier otro item lanza `Cannot determine type of 'item'`. `input_file` no está soportado. |
| llama-server: `previous_response_id` | Rechazado explícitamente: `throw std::invalid_argument("llama.cpp does not support 'previous_response_id'.")`. |
| llama-server: herramientas | Solo `type: "function"`. Otros tipos de tool se omiten con warning. `max_output_tokens` → `max_tokens`. `reasoning.effort` → `reasoning_effort`. |
| llama-server: SSE de Responses | `tools/server/server-common.cpp`, `format_oai_resp_sse`: escribe `event: <nombre>\ndata: <json>\n\n`. **No emite `data: [DONE]`.** El stream termina con `response.completed` y el cierre de la conexión. |
| llama-server: eventos | `response.created`, `response.in_progress`, `response.output_item.added`, `response.content_part.added`, `response.output_text.delta`, `response.reasoning_text.delta`, `response.function_call_arguments.delta`, `response.output_text.done`, `response.content_part.done`, `response.output_item.done`, `response.completed`. |
| llama-server: errores | `tools/server/server.cpp`, `ex_wrapper`: `std::invalid_argument` → HTTP 400 con `{"error":{"message":...,"type":"invalid_request_error","code":400}}`; otras excepciones → 500. |
| llama-server: contrato probado | `tools/server/tests/unit/test_compat_oai_responses.py` (llama.cpp v0.4.0) valida con la librería `openai` de Python los tres modos: JSON, streaming y telemetría. El test afirma `res.id.startswith("resp_")`, `res.output[0].id.startswith("msg_")` y la secuencia de eventos del streaming. |
| LARM: proxy OpenAI | `internal/api/proxy.go`, `Handler.serveInference` es el mecanismo **único** de ejecución: resolución de modelo, `RequestWithContext` (lease automática), `inferences.Create`, `WaitActive`, `BeginInference`, `AssignProcess`, proxy HTTP a `model.Host:model.Port` + `path`, y liberación en el `defer` (`ReleaseAutomatic` / `Release`). El cuerpo de la petición se reenvía **byte a byte** y solo se fijan `Content-Type` y `Accept`. `Authorization` nunca se copia. |
| LARM: parser SSE | `internal/inference/sse.go`. `SSEParser` retiene solo la línea actual y un flag de `[DONE]`. No bufferea el cuerpo. |
| LARM: Scheduler y leases | `internal/scheduler/scheduler.go`: `RequestWithContext`, `WaitActive`, `BeginInference`, `ReleaseAutomatic`, `Release`. `SetReleaseProtector` y `SetAdmissionGate` enlazan Scheduler ↔ Inference Registry. |
| LARM: Execution Gate | `internal/executiongate/coordinator.go`. Externo al proxy HTTP: interviene en el arranque del proceso del backend, no en la ruta de la petición. |
| LARM: Recovery | `internal/recovery/service.go`. Bloquea la admisión con `ErrRecoveryRequired` a través del `AdmissionGate` del Scheduler. |
| LARM: router LAN | `internal/api/lan.go`. Allowlist estricta de método+ruta antes del mux, guardia Bearer (SHA-256 + `crypto/subtle.ConstantTimeCompare`) y CIDR antes de cualquier handler. `lanInference` descarta `X-LARM-Lease-ID` y `X-LARM-Request-ID`. |
| LARM: core contract | `internal/corecontract/types.go`, `SSEObservation.DoneDetected` es el flag que distingue un stream completo de uno truncado. `validate.go` rechaza `Truncated && DoneDetected` a la vez. |

---

## 2. Arquitectura

```
              ┌──────────────────────── proceso LARM ────────────────────────┐
              │                                                             │
  local ────► │  api.Handler (mux) ──► POST /v1/responses ──┐               │
              │                                            │               │
  LAN (TLS) ► │  api.newLANHandler ── allowlist ───────────┼──►            │
              │   Bearer + CIDR                          │   │            │
              │   descarta lease/request del cliente     │   │            │
              │                                         ▼   │            │
              │              Handler.serveInference(path) ◄──┘            │
              │                         │                                  │
              │                         ▼                                  │
              │              Scheduler ──► Runtime Manager ──► llama-server  │
              │              (FIFO, leases,          (proceso)   :puerto    │
              │               admission, gate)                       local  │
              │                         │                                  │
              │                         ▼                                  │
              │              Inference Registry                          │
              └─────────────────────────────────────────────────────────────┘
```

**Decisión: proxy transparente, no adaptador.** La inspección demuestra que la conversión ya la hace llama-server: su `/v1/responses` acepta el cuerpo Responses y devuelve eventos Responses. Adaptar en LARM significaría duplicar esa conversión y mantener dos contratos que divergirían con cada versión de llama.cpp. El proxy transparente preserva el contrato exacto y deja que el backend sea la única autoridad sobre lo que soporta.

La única diferencia real entre Chat Completions y Responses en el camino de LARM es **el terminador del stream**:

- Chat Completions: `data: [DONE]`
- Responses: `event: response.completed`

Todo lo demás —lease, espera FIFO, admisión, verificación de proceso, proxy, cancelación, liberación, propagación de errores— es idéntico porque vive en `serveInference`, que no conoce el formato del payload.

### 2.1 Archivos y cambios

| Archivo | Cambio |
|---|---|
| `internal/api/proxy.go` | Nuevo handler `Handler.responses` que llama a `serveInference(w, r, "/v1/responses", true)`. `serveInference` no se modificó. |
| `internal/api/handler.go` | Registro de `POST /v1/responses` en el mux local, junto a `/v1/chat/completions` y `/v1/embeddings`. |
| `internal/api/lan.go` | `/v1/responses` añadido a la allowlist LAN y al `switch` estricto de método+ruta. El adaptador `lanInference` se reutiliza sin cambios. |
| `internal/inference/sse.go` | `SSEParser` reconoce además `event: response.completed` como terminador. La detección de `[DONE]` queda intacta. |
| `internal/api/handler_test.go` | El backend falso sirve `/v1/responses` con payload JSON y stream SSE con el framing real de llama.cpp. Nuevo modo `toolcall`. |
| `internal/api/responses_test.go` | Nuevo. Suite de 20 pruebas de la Responses API. |
| `internal/inference/sse_test.go` | Nuevo. Pruebas del parser SSE con los dos terminadores. |
| `internal/api/lan_inference_test.go` | Actualizado: `/v1/responses` ya no es una ruta denegada en LAN (era una aserción de LARM-008.2). |
| `configs/codex/*.config.toml` | Nuevo. Perfiles de referencia de Codex para llama.cpp directo, LARM local y LARM LAN. |
| `docs/larm-008.3-codex-responses-api.md` | Nuevo. Este documento. |

**No se modificaron** el Scheduler, Recovery, Capacity Admission, el Execution Gate, la semántica de leases, el comportamiento de `/v1/chat/completions`, `/v1/embeddings`, `/v1/models` ni ninguna ruta administrativa.

---

## 3. Endpoints finales

| Listener | Método | Ruta | Autenticación | Estado |
|---|---|---|---|---|
| Local | `POST` | `/v1/responses` | contrato local vigente | **Nuevo** |
| Local | `GET` | `/v1/models` | contrato local vigente | Sin cambios |
| Local | `POST` | `/v1/chat/completions` | contrato local vigente | Sin cambios |
| Local | `POST` | `/v1/embeddings` | contrato local vigente | Sin cambios |
| LAN | `POST` | `/v1/responses` | TLS + Bearer + CIDR | **Nuevo** |
| LAN | `GET` | `/v1/models` | TLS + Bearer + CIDR | Sin cambios |
| LAN | `POST` | `/v1/chat/completions` | TLS + Bearer + CIDR | Sin cambios |
| LAN | `POST` | `/v1/embeddings` | TLS + Bearer + CIDR | Sin cambios |
| LAN | `GET` | `/api/v1/lan/health` | TLS + Bearer + CIDR | Sin cambios |
| LAN | *cualquier otro* | *cualquier otra* | — | `404 not found` |

El listener LAN **no** expone ninguna ruta administrativa nueva. La allowlist sigue siendo un `switch` estricto de método y ruta exacta evaluado antes del mux.

---

## 4. Contrato Responses API soportado

### 4.1 Petición

LARM reenvía el cuerpo sin filtrar. El contrato depende de lo que soporte llama-server 0.4.0:

| Campo | Soportado por llama-server | Observación |
|---|---|---|
| `model` | Sí | LARM lo usa además para resolver el modelo del registro. Un `model` inexistente devuelve `404` antes de tocar el backend. |
| `input` (string) | Sí | Se convierte a un mensaje `user`. |
| `input` (array) | Sí, con tipos limitados | `message`, `function_call`, `function_call_output`, `reasoning`. Otros tipos → HTTP 400 del backend. |
| `instructions` | Sí | Se convierte a un mensaje `system` inicial. Codex HTTP no lo usa: mete las instrucciones como item `message` de rol `developer`. |
| `stream` | Sí | `true` produce SSE; `false` produce JSON. |
| `tools` | Sí, solo `type: "function"` | Otros tipos de tool se omiten con warning en llama-server. |
| `tool_choice` | Sí | `auto` y `required` se mapean al Chat Completions equivalente. |
| `max_output_tokens` | Sí | Se traduce a `max_tokens`. |
| `reasoning` | Solo `effort` | Se traduce a `reasoning_effort`. |
| `store` | Ignorado | llama-server no lo implementa; se reenvía y se descarta. |
| `include` | Ignorado | Ídem. |
| `prompt_cache_key`, `prompt_cache_options` | Ignorados | Campos propios de la API de OpenAI. |
| `text` | Ignorado | Control de verbosidad/formato JSON de OpenAI. |
| `stream_options` | Ignorado | Específico de OpenAI. |
| `previous_response_id` | **Rechazado** | llama-server: "llama.cpp does not support 'previous_response_id'." → HTTP 400. Codex HTTP nunca lo envía (solo existe en el transporte WebSocket). |
| Campos desconocidos | Reenviados | LARM no filtra nada: el backend decide. |

**No se inventa soporte.** Lo que el backend no soporta, LARM lo reenvía y lo hace fallar con el error del backend, que es el contrato correcto de un proxy transparente.

### 4.2 Respuesta no streaming

llama-server devuelve el objeto `response`:

```json
{
  "id": "resp_...",
  "object": "response",
  "status": "completed",
  "model": "...",
  "created_at": 1730000000,
  "output": [
    {"id": "msg_...", "type": "message", "role": "assistant", "status": "completed",
     "content": [{"type": "output_text", "annotations": [], "logprobs": [], "text": "..."}]},
    {"id": "fc_...", "type": "function_call", "status": "completed",
     "arguments": "{...}", "call_id": "call_...", "name": "..."}
  ],
  "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0,
            "input_tokens_details": {"cached_tokens": 0}}
}
```

LARM lo reenvía íntegro.

### 4.3 Streaming SSE

Framing: `event: <nombre>\ndata: <json>\n\n`.

| Evento | `data.type` | Contenido relevante |
|---|---|---|
| `response.created` | `response.created` | `response.id` |
| `response.in_progress` | `response.in_progress` | `response.id`, opcional `prompt_progress` |
| `response.output_item.added` | `response.output_item.added` | `item` (message, reasoning o function_call) |
| `response.content_part.added` | `response.content_part.added` | `item_id`, `part` |
| `response.output_text.delta` | `response.output_text.delta` | `item_id`, `delta` |
| `response.reasoning_text.delta` | `response.reasoning_text.delta` | `item_id`, `delta` |
| `response.function_call_arguments.delta` | `response.function_call_arguments.delta` | `item_id`, `delta` |
| `response.output_text.done` | `response.output_text.done` | `item_id`, `text` |
| `response.content_part.done` | `response.content_part.done` | `item_id`, `part` |
| `response.output_item.done` | `response.output_item.done` | `item` completo |
| `response.completed` | `response.completed` | `response` con `output` y `usage` |

**No hay `data: [DONE]`.** El cierre del stream es el evento `response.completed` y el cierre de la conexión. Codex exige `response.completed`; si el stream termina sin él, Codex reporta `stream closed before response.completed`.

Los identificadores se preservan: `resp_...` en el objeto `response`, `msg_...` en los items de mensaje, `fc_...` y `call_...` en las llamadas a herramienta, `item_id` en los deltas.

### 4.4 Errores HTTP

| Situación | Respuesta |
|---|---|
| Cuerpo no JSON o sin `model` | `400` de LARM, cuerpo `{"error":{"message":"...","type":"server_error"}}` |
| Modelo inexistente en el registro | `404` de LARM, sin tocar el backend |
| Error del backend (p. ej. `previous_response_id`) | Se reenvía el **status y el cuerpo** del backend tal cual |
| Timeout de espera de cola | `504` de LARM |
| Cola llena | `429` de LARM |
| Cancelación del cliente | No se escribe cuerpo; la inferencia se cancela y la lease se libera |
| Timeout de inferencia | `504` de LARM |

---

## 5. Gestión del modelo y de las leases

El flujo es **el mismo código** que Chat Completions (`Handler.serveInference`), sin un scheduler paralelo:

1. Resolución del modelo contra `h.manager.Models()`. Un modelo inexistente devuelve `404` sin adquirir lease.
2. `h.inferences.Reserve(...)` reserva la identidad de petición; `X-LARM-Request-ID` del cliente se acepta solo en el listener local.
3. `h.scheduler.RequestWithContext(queueCtx, requestID, model)` — lease **automática**. El contexto de espera está acotado por `proxy.queue_wait_timeout` y por el plazo del llamante.
4. `h.inferences.Create(...)` — registro durable; si falla, `ProtectLease` impide la liberación prematura.
5. `h.scheduler.WaitActive(queueCtx, leaseID)` — espera FIFO. Un timeout devuelve `504`.
6. `h.scheduler.BeginInference(...)` — fija el `cancel` de la inferencia sobre la lease.
7. Verificación de la identidad del proceso (`CurrentProcessForModel`) y `AssignProcess`.
8. Proxy HTTP a `llama-server` en `model.Host:model.Port` con la ruta `/v1/responses`.
9. Liberación en el `defer`: `ReleaseAutomatic` si la inferencia fue aceptada, `Release` en caso contrario.

Capacity Admission, Recovery y Execution Gate intervienen en los mismos puntos que en Chat Completions: la admisión preventiva y la espera de capacidad viven dentro del Scheduler; Recovery bloquea la adquisición con `ErrRecoveryRequired`; el Execution Gate actúa en el arranque del proceso del backend.

**No existe un scheduler independiente.** La ruta Responses comparte el mismo `*api.Handler`, el mismo `*scheduler.Scheduler`, el mismo `*runtime.Manager`, el mismo `*inference.Registry` y el mismo cliente HTTP del backend que Chat Completions.

---

## 6. Streaming, cancelación y liberación

### 6.1 Entrega progresiva

El bucle de streaming de `serveInference` es el preexistente de LARM-008.2 y no se modificó:

- Lee del backend en bloques de 32 KiB, escribe y hace `Flush` en cada iteración.
- **No** almacena la respuesta completa en memoria antes de enviarla.
- `Content-Type: text/event-stream` propagado desde el backend, más `Cache-Control: no-cache` y `X-Accel-Buffering: no`.
- El nombre del evento SSE y el JSON de `data:` se copian sin transformar.

### 6.2 Terminador del stream

El `SSEParser` de `internal/inference/sse.go` ahora reconoce dos terminadores:

| Terminador | Superficie | Previo |
|---|---|---|
| `data: [DONE]` | Chat Completions | Ya existente, **sin cambios** |
| `event: response.completed` | Responses API | **Nuevo** |

La detección se hace sobre la línea `event:`. El parser sigue reteniendo solo la línea actual: no se introduce ningún buffereo de la respuesta.

Consecuencia semántica: un stream de Responses cortado antes de `response.completed` se registra como `SemanticTruncated` con `ErrorSSETruncated`, no como éxito. Esto es lo correcto y coincide con el contrato del `corecontract` (`Truncated && DoneDetected` es inválido).

### 6.3 Cancelación

- `r.Context().Done()` se comprueba antes de leer, después de leer, después de escribir y después del flush.
- La desconexión marca `MarkDownstreamDisconnected` y cancela la inferencia.
- La cancelación llega al backend porque el contexto de la inferencia es el de la petición.

### 6.4 Liberación de leases

- Finalización normal: `ReleaseAutomatic(leaseID, requestID, model)` en el `defer`.
- Cancelación o fallo antes de la admisión durable: `Release(leaseID)`.
- No quedan inferencias huérfanas: el `defer` corre en todos los caminos de retorno, incluidos los de error del backend y de desconexión del cliente.

---

## 7. Compatibilidad de Codex

### 7.1 Requisitos de Codex CLI 0.169.0

| Requisito | Valor |
|---|---|
| Versión | `codex-cli 0.169.0` |
| Wire API | `wire_api = "responses"` (obligatorio; `"chat"` fue eliminado) |
| Ruta | `base_url` + `/responses` |
| Base URL local | `http://127.0.0.1:8090/v1` |
| Base URL LAN | `https://<host-lan>:8443/v1` |
| Autenticación local | Ninguna (`requires_openai_auth = false`, sin `env_key`) |
| Autenticación LAN | `env_key = "LARM_LAN_API_KEY"` → `Authorization: Bearer <clave>` |
| Certificados TLS | `CODEX_CA_CERTIFICATE=/path/to/larm-ca-cert.pem` (fallback: `SSL_CERT_FILE`) |
| Contexto | `model_context_window = 98304` |
| Auto compactación | `model_auto_compact_token_limit = 85000` |
| WebSocket | `supports_websockets = false` (llama-server no lo implementa) |
| Compactación remota | `capabilities = { remote_compaction = "unsupported" }` |

### 7.2 Compatibilidad de la petición

| Item / campo que envía Codex | Soportado por llama.cpp | Resultado |
|---|---|---|
| `input[].type = "message"`, `role ∈ {user, system, developer, assistant}` | Sí | ✅ |
| `input[].content[] = {"type":"input_text","text":...}` | Sí | ✅ |
| `input[].type = "function_call"` (`arguments`, `call_id`, `name`) | Sí | ✅ |
| `input[].type = "function_call_output"` con `output` como texto | Sí | ✅ |
| `input[].type = "function_call_output"` con `output` como array de `{"type":"input_text"}` | Sí | ✅ |
| `input[].type = "reasoning"` | Sí | ✅ |
| `input[].type = "additional_tools"` | **No** | ❌ HTTP 400 del backend |
| `tools` de primer nivel (`type: "function"`) | Sí | ✅ |
| `tool_choice`, `parallel_tool_calls` | Sí | ✅ |
| `store`, `include`, `prompt_cache_key`, `text`, `stream_options` | Ignorados por el backend | ✅ (se reenvían) |
| `previous_response_id` | No | No se envía nunca en HTTP |

### 7.3 Mitigación del item `additional_tools`

Codex CLI 0.169.0 tiene la feature `incremental_tools` en estado `stable` y activa por defecto. Cuando está activa, Codex inserta en el array `input` un item `{"type":"additional_tools","role":"developer","tools":[...]}`. llama.cpp 0.4.0 no conoce ese tipo de item y responde HTTP 400 con `Cannot determine type of 'item'`.

Verificado empíricamente con el Codex instalado:

```bash
codex debug prompt-input "hola"
# -> 7 items, uno de ellos type=additional_tools

codex --disable incremental_tools debug prompt-input "hola"
# -> 5 items, todos type=message (roles developer/user, content input_text)
```

Con `incremental_tools` desactivado, **todos** los items del array `input` son del tipo que llama.cpp soporta. Por eso los perfiles de referencia de la sección 8 no lo activan y la sección 8.3 documenta el flag explícitamente.

Esta es una incompatibilidad **entre Codex CLI y llama.cpp**, no entre Codex y LARM: LARM reenvía el item sin filtrar, que es el comportamiento correcto de un proxy transparente. Filtrarlo en LARM rompería el contrato de "no descartar campos que el backend soporte" y ocultaría una decisión que pertenece al backend.

### 7.4 Compatibilidad de la respuesta

| Evento que emite llama.cpp | Lo consume Codex | Resultado |
|---|---|---|
| `response.created` | Sí (`ResponseEvent::Created`) | ✅ |
| `response.in_progress` | Sí, ignorado con trace | ✅ |
| `response.output_item.added` | Sí (`ResponseEvent::OutputItemAdded`) | ✅ |
| `response.content_part.added` | Sí, ignorado | ✅ |
| `response.output_text.delta` | Sí (`ResponseEvent::OutputTextDelta`) | ✅ |
| `response.output_text.done` | Sí, ignorado | ✅ |
| `response.content_part.done` | Sí, ignorado | ✅ |
| `response.output_item.done` | Sí (`ResponseEvent::OutputItemDone`) —.porta las tool calls | ✅ |
| `response.function_call_arguments.delta` | Sí, ignorado (Codex usa el item completo) | ✅ |
| `response.reasoning_text.delta` | Sí, pero **descartado**: Codex exige `content_index` y llama.cpp no lo emite | ⚠️ |
| `response.completed` | Sí (`ResponseEvent::Completed`) — **obligatorio** | ✅ |

Codex decide por el campo `type` dentro del JSON de `data:`. llama-server escribe ese campo en el payload de `data:` además del nombre del evento, así que el contrato se cumple.

El `usage` que emite llama-server (`input_tokens`, `output_tokens`, `total_tokens`, `input_tokens_details.cached_tokens`) encaja con `ResponseCompletedUsage` de Codex. Codex no exige `output_tokens_details`.

### 7.5 `previous_response_id` y almacenamiento de respuestas

| Pregunta | Respuesta verificada |
|---|---|
| ¿Codex HTTP envía `previous_response_id`? | **No.** `ResponsesApiRequest` (el struct del transporte HTTP) no tiene ese campo. Solo `ResponseCreateWsRequest` (WebSocket) lo tiene, y Codex lo pone a `None`. |
| ¿Codex depende de `store`? | Envía `store: false`/configurable, pero **no** usa la respuesta almacenada: reenvía el historial completo en `input` en cada turno. |
| ¿llama.cpp soporta `previous_response_id`? | No, lo rechaza explícitamente con HTTP 400. |
| ¿El transporte WebSocket es necesario? | No. `supports_websockets = false` lo desactiva, y llama-server no lo implementa. |

**Conclusión:** la incompatibilidad de `previous_response_id` **no es alcanzable** por Codex CLI sobre HTTP ni sobre llama.cpp. No se declara PASS para un escenario que no puede ocurrir.

---

## 8. Configuraciones de Codex (preparadas, no ejecutadas)

Los perfiles viven en `configs/codex/` y **no** tocan la configuración operativa del usuario. El contrato de Codex CLI 0.169.0 es: `-p/--profile <name>` superpone `$CODEX_HOME/<name>.config.toml` sobre la configuración base. Por eso los archivos se llaman `<name>.config.toml` y no usan una sección `[profiles.<name>]`.

### 8.1 llama.cpp directo

Archivo: `configs/codex/llama-direct.config.toml`

| Aspecto | Valor |
|---|---|
| Model ID | `gpt-oss-20b` |
| Base URL | `http://127.0.0.1:8060/v1` |
| Wire API | `responses` |
| Contexto | `98304` (`model_auto_compact_token_limit = 85000`) |
| Autenticación | Ninguna (`requires_openai_auth = false`) |
| TLS | No aplica (http) |
| Limitación | llama-server sin `--api-key`; el mismo proceso sirve `/v1/responses` nativo |

### 8.2 LARM local

Archivo: `configs/codex/larm-local.config.toml`

| Aspecto | Valor |
|---|---|
| Model ID | `code-agent` (el id del registro LARM; `alias: gpt-oss-20b`) |
| Base URL | `http://127.0.0.1:8090/v1` |
| Wire API | `responses` |
| Contexto | `98304` (`model_auto_compact_token_limit = 85000`) |
| Autenticación | Ninguna (contrato local vigente) |
| TLS | No aplica (http) |
| Limitación | El id de modelo debe coincidir con el id del registro LARM, no con el alias |

### 8.3 LARM LAN

Archivo: `configs/codex/larm-lan.config.toml`

| Aspecto | Valor |
|---|---|
| Model ID | `code-agent` |
| Base URL | `http://192.169.0.228:8443/v1` |
| Wire API | `responses` |
| Contexto | `98304` (`model_auto_compact_token_limit = 85000`) |
| Autenticación | `env_key = "LARM_LAN_API_KEY"` → `Authorization: Bearer <clave>` |
| Certificados TLS | No requeridos cuando `server.lan.tls.enabled: false` |
| Limitaciones | El host, el puerto y la allowlist CIDR son los de `server.lan` en la configuración de LARM. El cliente debe estar dentro de la allowlist. La clave es compartida por todos los clientes LAN. |

Los tres perfiles se validaron con el Codex instalado sin ejecutar ninguna inferencia:

```bash
CODEX_HOME=<dir-temporal-con-los-perfiles> codex doctor --summary
# [ok] config       loaded
```

### 8.4 Uso previsto (requiere autorización de prueba física)

```bash
# 1) llama.cpp directo
cp configs/codex/llama-direct.config.toml "$CODEX_HOME/llama-direct.config.toml"
codex --disable incremental_tools -p llama-direct

# 2) LARM local
cp configs/codex/larm-local.config.toml "$CODEX_HOME/larm-local.config.toml"
codex --disable incremental_tools -p larm-local

# 3) LARM LAN
cp configs/codex/larm-lan.config.toml "$CODEX_HOME/larm-lan.config.toml"
export LARM_LAN_API_KEY='<clave de operador>'
codex --disable incremental_tools -p larm-lan
```

`--disable incremental_tools` es necesario mientras llama.cpp no soporte el item `additional_tools` (sección 7.3).

---

## 9. Seguridad LAN

Se mantienen intactas las protecciones de LARM-008.1 y LARM-008.2:

| Control | Comportamiento verificado |
|---|---|
| Allowlist estricta | `/v1/responses` es la única ruta nueva. Cualquier otra ruta, método alternativo, barra final o prefijo devuelve `404` antes de tocar el mux. |
| Bearer | `Authorization: Bearer <clave>` comparado en tiempo constante contra los hashes SHA-256 en memoria. Sin cabecera o con clave inválida: `401` + `WWW-Authenticate: Bearer`. |
| CIDR | La dirección de origen se comprueba contra `server.lan.access.allow`. Fuera: `403 forbidden`. |
| Orden de los controles | Bearer y CIDR se evalúan **antes** de adquirir lease. Un `401`/`403` no crea estado en el Scheduler. |
| Credenciales no reenviadas | `Authorization` nunca se copia a la petición al backend. El backend falso lo devuelve en el payload y el test comprueba que llega vacío. |
| `X-LARM-Lease-ID` | Descartado por `lanInference`. El cliente LAN siempre toma el camino de lease automática. |
| `X-LARM-Request-ID` | Descartado por `lanInference`. El cliente LAN no puede colisionar con una identidad local ni consultar inferencias ajenas. |
| `X-LARM-Execution-ID` | Se conserva y lo valida el proxy compartido (UUIDv4 canónico). El registro de inferencia se indexa por `requestID`, que es generado por LARM en LAN: un `Execution-ID` compartido por dos clientes produce dos registros independientes y no da acceso a la sesión ajena. |
| Identificadores de respuesta | El payload de la Responses API solo contiene datos del backend (`resp_`, `msg_`, `fc_`, `call_`). LARM no expone `X-LARM-Lease-ID` ni ninguna identidad de lease en la respuesta. |
| Rutas administrativas | `404` desde LAN: `/api/v1/status`, `/api/v1/models`, `/api/v1/processes`, `/api/v1/leases`, `/api/v1/recovery/*`, `/api/v1/capacity/*`, `/api/v1/admission/*`, `/api/v1/models/{id}/start`, `/api/v1/models/{id}/stop`, `/api/v1/inferences`, `/api/v1/queue`, `/health`. |
| Transporte | Con la configuración operativa actual (`server.lan.tls.enabled: false`) el listener LAN sirve HTTP; no requiere certificados en los clientes. |

---

## 10. Matriz de pruebas

Todos los tests usan el backend falso controlado por `LARM_API_BACKEND_MODE` y un `Manager` + `Scheduler` reales sobre un binario falso. **No se carga ningún GGUF real.**

### 10.1 `internal/api/responses_test.go` (nuevo)

| # | Escenario | Test | Resultado |
|---|---|---|---|
| 1 | Respuesta JSON no streaming con identidad y tool call | `TestResponsesJSONResponse` | PASS |
| 2 | Streaming SSE con nombres de evento e identificadores | `TestResponsesStreamingSSEDeliversProgressiveEvents` | PASS |
| 3 | Streaming progresivo (el primer evento llega con el backend vivo) | `TestResponsesStreamingIsProgressive` | PASS |
| 4 | Tool calling (`response.function_call_arguments.delta`, `call_id`, `name`) | `TestResponsesToolCallingEvents` | PASS |
| 5 | Múltiples eventos de una misma respuesta con identidades distintas | `TestResponsesMultipleEventsOfOneResponse` | PASS |
| 6 | Cancelación por desconexión: libera lease y deja la inferencia ambigua | `TestResponsesClientCancellationReleasesLease` | PASS |
| 7 | Error del backend: status preservado y lease liberada | `TestResponsesBackendErrorIsPropagated` | PASS |
| 8 | Modelo inexistente: `404` sin lease | `TestResponsesUnknownModelIsNotFound` | PASS |
| 9 | Timeout del Scheduler: `504` y cola vacía | `TestResponsesSchedulerTimeout` | PASS |
| 10 | Solicitudes concurrentes sobre el mismo Scheduler | `TestResponsesConcurrentRequestsShareTheScheduler` | PASS |
| 11 | Liberación de lease y estado de éxito de la inferencia | `TestResponsesReleasesTheAutomaticLease` | PASS |
| 12 | Stream truncado: `SemanticTruncated`, no éxito | `TestResponsesTruncatedStreamIsNotSuccess` | PASS |
| 13 | Campos de Responses API no soportados: se reenvían sin filtrar | `TestResponsesUnsupportedFieldsAreForwarded` | PASS |
| 14 | Chat Completions y embeddings sin cambios | `TestResponsesKeepsChatCompletionsAndEmbeddings` | PASS |
| 15 | Identificadores de respuesta no exponen identidades de lease | `TestResponsesLeaseIsNotReleasedByAnotherClient` | PASS |
| 16 | LAN: autenticación obligatoria en `/v1/responses` | `TestLANResponsesRequiresAuthentication` | PASS |
| 17 | LAN: CIDR obligatorio antes de adquirir lease | `TestLANResponsesRejectsAddressOutsideAllowlist` | PASS |
| 18 | LAN: ruta autorizada con el mismo contrato que la local | `TestLANResponsesAuthorized` | PASS |
| 19 | LAN: el Bearer no llega al backend | `TestLANResponsesDoesNotForwardTheBearerKeyToTheBackend` | PASS |
| 20 | LAN: lease e identidad del cliente descartados | `TestLANResponsesClientLeaseAndRequestIdentityAreDropped` | PASS |
| 21 | LAN: `X-LARM-Execution-ID` no interfiere entre clientes | `TestLANResponsesExecutionIDDoesNotInterfere` | PASS |

### 10.2 `internal/inference/sse_test.go` (nuevo)

| # | Escenario | Test | Resultado |
|---|---|---|---|
| 22 | `[DONE]` de Chat Completions sigue detectado | `TestSSEParserDetectsChatCompletionsDone` | PASS |
| 23 | `response.completed` de Responses es detectado | `TestSSEParserDetectsResponsesCompleted` | PASS |
| 24 | Stream de Responses sin evento terminal no se marca completo | `TestSSEParserResponsesStreamWithoutTerminalEvent` | PASS |
| 25 | Otros nombres de evento no terminan el stream | `TestSSEParserIgnoresOtherEventNames` | PASS |

### 10.3 Regresiones de LARM-008.2 actualizadas

| # | Escenario | Test | Resultado |
|---|---|---|---|
| 26 | `/v1/responses` ya no es una ruta administrativa denegada en LAN | `TestLANAdministrativeRoutesAreDenied` | PASS (actualizado) |
| 27 | Métodos alternativos sobre `/v1/responses` denegados | `TestLANAlternativeMethodsAreDenied` | PASS (actualizado) |
| 28 | Resto de la suite de regresión de LARM-008.2 | suite completa de `internal/api` | PASS |

### 10.4 Evidencia de streaming

`TestResponsesStreamingIsProgressive` arranca el backend en modo `hangstream` (el backend escribe el primer evento y luego espera indefinidamente). El test lee del socket del cliente y exige que `response.created` llegue **mientras el backend sigue vivo**. Si el proxy bufferease la respuesta, la lectura bloquearía hasta el timeout del backend y el test fallaría.

### 10.5 Evidencia de tool calling

`TestResponsesToolCallingEvents` usa el modo `toolcall` del backend falso, que emite la secuencia real de llama-server:

```
response.output_item.added           (item: function_call, call_id, name)
response.function_call_arguments.delta
response.output_item.done            (item: function_call completed)
response.completed
```

El test comprueba la presencia del evento de argumentos, la del `call_id`, la del `name` y el `response.completed` final.

### 10.6 Evidencia de liberación de leases

`TestResponsesReleasesTheAutomaticLease` comprueba, tras una respuesta streaming completa:

- `sched.ActiveInferenceCount() == 0`
- `sched.Snapshot().Active == nil` y `Pending` vacío
- El registro de inferencia está en `SemanticSucceeded` / `TransportCompleted`
- `SSE.DoneDetected == true`, `SSE.EOF == true`, `SSE.Truncated == false`

---

## 11. Limitaciones

1. **Item `additional_tools` de Codex**: llama.cpp 0.4.0 lo rechaza (HTTP 400). Mitigación: `--disable incremental_tools`. No es una limitación de LARM: el proxy reenvía fielmente el campo.
2. **`previous_response_id`**: llama.cpp lo rechaza. No es alcanzable desde Codex CLI sobre HTTP. Un cliente que lo envíe directamente recibirá el `400` del backend.
3. **Almacenamiento de respuestas (`store`)**: llama.cpp no lo implementa. Codex HTTP no lo necesita.
4. **Compactación remota**: es un protocolo de OpenAI. Los perfiles la declaran `unsupported` para que Codex use la compactación local.
5. **Transporte WebSocket de Responses**: llama.cpp no lo implementa. `supports_websockets = false`.
6. **`instructions` de primer nivel**: Codex no lo usa (mete las instrucciones como item `developer`). llama.cpp lo soporta si se envía.
7. **Tools que no sean `function`**: llama.cpp los omite con warning. Codex solo envía tools `function`.
8. **`input_file`**: llama.cpp no lo soporta (`'input_file' is not supported by llamacpp at this moment`).
9. **Llamadas a herramienta personalizadas (`custom_tool_call`)**: Codex las usa para `exec`/`apply_patch`. Sus items de salida (`custom_tool_call_output`) no están en el conversor de llama.cpp. Solo se ha validado el camino `function` / `function_call_output`.
10. **El proxy no interpreta la Responses API**: no valida el esquema del cuerpo. La validación es del backend.
11. **`response.failed` / `response.incomplete`**: llama-server no los emite. Codex los toleraría, pero no se pueden observar.
12. **Los campos desconocidos no se descartan**: si el backend los ignora, LARM también; si el backend los rechaza, LARM propaga el error.
13. **El razonamiento en streaming no llega a Codex**: llama.cpp emite `response.reasoning_text.delta` con `delta` e `item_id` pero sin `content_index`. Codex solo acepta ese evento si vienen `delta` **y** `content_index`, así que descarta el delta. El razonamiento sigue estando completo en el item `reasoning` de `response.output_item.done`, que Codex sí consume, pero no se ve progresivamente.

---

## 12. Riesgos residuales

1. **El item `additional_tools` rompe la primera petición si se olvida el flag.** Es el riesgo operativo principal. Mitigación: documentar `--disable incremental_tools` en todos los perfiles y en el manual de uso. Riesgo de recurrencia: cada actualización de Codex que añada items al array `input` puede volver a activar el problema. LARM no lo detecta ni lo oculta.
2. **El proxy transparente propaga cualquier error del backend sin clasificarlo.** Un `400` de llama.cpp por un campo no soportado llega al cliente tal cual, con el mensaje del backend. Es el contrato correcto, pero el mensaje puede revelar detalles internos del backend.
3. **Allowlist LAN por CIDR sin identidad de host.** La pertenencia a la red más el secreto compartido es la única prueba de origen. La custodia y rotación de la clave quedan fuera de LARM.
4. **Clave LAN compartida.** No hay revocación granular ni lista dinámica. Un cliente compromise committe la clave.
5. **Superficie LAN compartida con el listener local.** Un cliente LAN puede retrasar a uno local ocupando la cola. Es el comportamiento pedido —un único mecanismo de ejecución— pero es un riesgo de denegación de servicio dentro de la LAN autorizada.
6. **El timeout de inactividad de stream (`stream_idle_timeout`) cancela la inferencia.** Un stream de Responses lento (por ejemplo, el razonamiento largo de GPT-OSS con `reasoning_effort = medium`) puede superar el valor por defecto de 2 minutos entre eventos y ser cancelado. Codex reintenta, pero el trabajo se pierde. Riesgo real: el evento `response.reasoning_text.delta` de GPT-OSS puede tener pausas largas.
7. **Sin soporte de `usage.output_tokens_details`.** Codex lo espera opcionalmente; su ausencia no rompe nada, pero las métricas de tokens de razonamiento pueden quedar a cero.
8. **La Responses API de llama-server es una capa de compatibilidad sobre Chat Completions.** Cualquier campo que llama.cpp no sepa traducir se pierde en el camino inverso (la respuesta), no en el de ida.
9. **No se ejecutaron pruebas físicas.** No se abrió ningún puerto, no se cargó ningún GGUF real, no se arrancó ningún llama-server y no se validó el comportamiento con el Codex CLI real ni con hardware de red real. La compatibilidad con Codex está **verificada por inspección del código fuente de ambas versiones** y por la suite automática con backend falso, no por una ejecución real.
10. **El id de modelo de Codex debe ser el id del registro LARM**, no el alias. `code-agent` es el id; `gpt-oss-20b` es su alias. Usar el alias con LARM devolvería `404`.

---

## 13. Regresiones

| Comando | Exit code |
|---|---|
| `go test ./... -count=1` | `0` |
| `go test -race ./... -count=1` | `0` |
| `go vet ./...` | `0` |
| `git diff --check` | `0` |

No hubo fallos preexistentes que documentar.

---

## 14. Estado

**PASS** para el alcance de LARM-008.3:

- `POST /v1/responses` implementado en el listener local y en el listener LAN autenticado.
- Proxy transparente: el contrato Responses de llama.cpp se preserva sin adaptación de mensajes ni de eventos.
- Scheduler, Recovery, Capacity Admission, Execution Gate y la semántica de leases reutilizados sin cambios.
- Streaming SSE progresivo con los nombres de evento y los identificadores de Responses.
- Tool calling preservado.
- Cancelación propagada al backend y lease liberada.
- Seguridad LAN de LARM-008.1 y LARM-008.2 intacta.
- 25 pruebas automatizadas nuevas (21 de la Responses API y 4 del parser SSE) y 2 pruebas de LARM-008.2 actualizadas, todas en PASS.
- Configuraciones de Codex preparadas para los tres destinos, sin tocar la configuración operativa.

**Pendiente de autorización**: pruebas físicas con Codex CLI y llama-server real (sección 8.4).

**Detenido en LARM-008.3.**
