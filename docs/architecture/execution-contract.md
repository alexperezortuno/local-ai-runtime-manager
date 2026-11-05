# Contrato de ejecución explícita LARM — versión 1

**Estado:** propuesta de diseño para LARM-004.2.3. No es una API implementada ni una capacidad nativa de OpenCode. El contrato evita derivar identidad del orden de eventos, timestamps, agente, modelo o `session_id`.

## 1. Decisión y límites

La unidad de propiedad es una **ejecución lógica** identificada por un UUID creado por quien la inicia. Sus solicitudes de inferencia tienen IDs distintos y explícitos; la lease LARM es un recurso separado que una ejecución posee temporalmente. Un `execution_id` no es un `session_id`, `request_id` ni `lease_id`.

Arquitectura recomendada para la integración futura:

1. Un **wrapper/launcher** crea la identidad raíz antes de iniciar una ejecución y conserva un journal durable.
2. Un **bridge/plugin cooperante** recibe contexto explícito de ejecución; propaga identidad y un ID nuevo por intento HTTP. No la reconstruye de hooks incompletos.
3. Un **adaptador HTTP local** es el único propietario de las leases, registra la vida real de cada respuesta y usa la API LARM existente.

Sin contexto explícito no hay lease administrada por ejecución: el modo compatible es la lease automática por inferencia de LARM. La integración debe fallar cerrada para el modo administrado, no elegir una ejecución por aproximación.

## 2. Sobre versionado y entidades

`schema_version` es la cadena fija `"1"`. Los objetos son JSON UTF-8, UUID canónicos en minúscula (UUIDv4 generado criptográficamente) y tiempos RFC 3339 UTC. Los ejemplos omiten campos de transporte que no son necesarios en todos los mensajes.

### 2.1 Ejecución

```json
{
  "schema_version": "1",
  "execution_id": "9bb83c70-881d-4cac-93cb-b116cd4e8bd0",
  "parent_execution_id": null,
  "session_id": null,
  "state": "CREATED",
  "created_at": "2026-11-02T18:00:00Z",
  "initiator": "larm-opencode-wrapper"
}
```

| Campo | Requisito y validación |
|---|---|
| `schema_version` | Obligatorio; solo `"1"`. Rechazar versiones desconocidas. |
| `execution_id` | Obligatorio; UUIDv4 globalmente único dentro del coordinador/journal. Se genera antes de iniciar trabajo. |
| `parent_execution_id` | Opcional/null para raíz; para hijo, UUID existente y distinto de sí mismo. Rechazar ciclos. |
| `session_id` | Opcional, opaco y solo metadata/route handle tras un bind explícito. Nunca genera, reemplaza ni identifica por sí solo `execution_id`. |
| `state` | Obligatorio; uno de los estados de §3. No aceptar saltos de transición. |
| `created_at` | Obligatorio; auditoría solamente, nunca identidad, orden causal ni criterio de correlación. |
| `initiator` | Obligatorio; ID estable del componente iniciador, no el nombre del agente/modelo. |
| `agent_id` | Opcional; atributo descriptivo. No es clave. |

Los intentos de creación se pueden repetir con el mismo UUID solo si el cuerpo es idéntico; devolver el registro existente. Reutilizar el UUID con distinto padre/initiator es conflicto. Un hijo recibe un UUID nuevo **antes** de que comience; nunca hereda el ID del padre.

### 2.2 Solicitud de inferencia

```json
{
  "schema_version": "1",
  "execution_id": "9bb83c70-881d-4cac-93cb-b116cd4e8bd0",
  "request_id": "72eed7d1-5cec-4b65-9479-3e1de12bc271",
  "model_id": "code-fast",
  "kind": "primary",
  "stream": true,
  "state": "SENT"
}
```

`execution_id`, `request_id`, `model_id`, `stream` y `state` son obligatorios. `request_id` es UUIDv4 nuevo por cada intento HTTP, incluidos retries; el retry conserva `execution_id`. `kind` es opcional y descriptivo. No se usa `session_id`, modelo ni orden para hacer join.

En el transporte cooperante se propagan `X-LARM-Execution-ID` y `X-LARM-Request-ID`; `X-LARM-Parent-Execution-ID` es opcional y solo metadata de árbol. El adaptador valida formato, existencia/estado del ID de ejecución y unicidad de `request_id`. Para proveedor LARM se añade `X-LARM-Lease-ID` solo después de validar lease ACTIVE y modelo exacto. Un cliente sin contexto explícito sigue el camino automático existente.

### 2.3 Enlace de lease

```json
{
  "schema_version": "1",
  "execution_id": "9bb83c70-881d-4cac-93cb-b116cd4e8bd0",
  "lease_id": "2ef8d7ef-b335-4404-b72c-5b2c08a36101",
  "model_id": "code-fast",
  "state": "ACTIVE"
}
```

Los cuatro campos son obligatorios cuando hay enlace. `lease_id` lo asigna LARM; el adaptador no lo inventa ni lo comparte entre ejecuciones. Se permite como máximo un lease no terminal por ejecución, y no se admite una solicitud cuyo `model_id` no coincida con la lease activa. Una lease cedida se libera; al readquirir se crea un enlace nuevo y normalmente un `lease_id` nuevo.

El journal relaciona `execution_id → request_id* → lease_id*`; no se añade esa propiedad de identidad al contrato de LARM.

## 3. Ciclo de vida

### 3.1 Estado de ejecución

Estados: `CREATED`, `WAITING_MODEL`, `RUNNING`, `WAITING_CHILDREN`, `YIELDED`, `FINALIZING`, `COMPLETED`, `CANCEL_REQUESTED`, `CANCELLED`, `FAILED`, `RECOVERY_REQUIRED`.

Transiciones válidas principales:

```text
CREATED → WAITING_MODEL → RUNNING
RUNNING ↔ WAITING_MODEL
RUNNING → WAITING_CHILDREN → WAITING_MODEL | RUNNING
RUNNING → YIELDED → WAITING_MODEL | RUNNING
RUNNING | WAITING_MODEL | WAITING_CHILDREN → FINALIZING → COMPLETED
estado no terminal → CANCEL_REQUESTED → CANCELLED
estado no terminal → FAILED
reinicio/resultado ambiguo → RECOVERY_REQUIRED → estado reconciliado | FAILED
```

`WAITING_CHILDREN` es para una llamada foreground que espera resultados. Con hijo background el padre puede seguir en `RUNNING`, pero el coordinador establece `model_gate=DESCENDANTS_ACTIVE`: solicitudes del padre esperan localmente y **no** encolan una lease. Crear una sesión hija no demuestra que el padre esté esperando.

### 3.2 Estado de lease

El adaptador persiste el estado que devuelve LARM: `QUEUED`, `PREPARING`, `ACTIVE`, `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`; agrega estados locales `ACQUIRE_UNKNOWN`, `RELEASE_PENDING` y `YIELDED` cuando la operación HTTP o la cesión aún no se puede confirmar. El estado de ejecución no se deduce del estado de lease, ni viceversa.

La petición LARM tiene estados locales por `request_id`: `CREATED → SENT → HEADERS → STREAMING → SUCCEEDED | FAILED | CANCELLED`; también `UNKNOWN` ante una pérdida ambigua. HTTP headers disponibles (`HEADERS`) **no** son término. Para SSE, éxito exige término semántico (`[DONE]` cuando el protocolo lo requiere) y EOF del body. EOF sin término semántico es respuesta truncada/`FAILED`, pero el transporte ya terminó. Para JSON, EOF completo cierra la solicitud.

### 3.3 Responsables

| Transición/acción | Autoridad |
|---|---|
| Crear raíz y journal inicial | Wrapper antes del proceso. |
| Crear hijo y pasar su contexto | Componente que despacha el hijo (bridge/coordinador); debe ocurrir antes del trabajo del hijo. |
| `WAITING_MODEL` / solicitud y EOF | Adaptador HTTP, según su request real. |
| `RUNNING`, terminal normal/error/interrupción | Bridge de ciclo de vida con el mismo `execution_id` explícito; el wrapper confirma el fin del proceso raíz. |
| Adquirir, heartbeat, ceder y liberar lease | Adaptador, único dueño de la lease. |
| Recuperación | Coordinador leyendo journal y consultando LARM; estados ambiguos no se convierten en éxito por timeout local. |

## 4. Propiedad y propagación de identidad

### Raíz

El wrapper crea el UUIDv4 inmediatamente antes de aceptar/iniciar cada trabajo raíz; persiste `CREATED` antes de lanzar OpenCode. Pasa el contexto por canal de invocación con alcance de ejecución (IPC/contexto de plugin firmado/local), no como variable global compartida entre trabajos. Una variable de entorno puede transportar el ID raíz a un proceso dedicado, pero no crea IDs hijos ni discrimina solicitudes concurrentes dentro de ese proceso.

### Hijos

El despachador de subagentes crea/solicita el `execution_id` hijo al aceptar la invocación, antes de que el hijo pueda pedir modelo. Lo enlaza explícitamente al padre y lo pasa como contexto por esa invocación. IDs de herramienta/call ID y `parentID` de sesión son metadatos útiles de la integración, no sustitutos del ID hijo. No se admiten dos hijos con el mismo ID, incluso si comparten modelo.

### Correlación HTTP

El bridge asigna un `request_id` UUIDv4 por intento en el hook pre-request y lo propaga con el `execution_id` hasta el adaptador; el adaptador conserva esos headers en su registro de request. El adaptador solo acepta contexto ya enlazado explícitamente a una ejecución. Requests paralelos se correlacionan por ID, no por el orden de `http.request`/`http.response`. Reintentos reciben nuevo `request_id`.

Un `session_id` puede servir como handle para encontrar contexto **después de un bind explícito** `execution_id ↔ session_id`; no se admite crear dicho bind por “sesión actual”, nombre de agente, modelo, timestamp o emparejamiento con evento cercano.

## 5. OpenCode v2.2.5: alcance comprobado y gate pendiente

La evidencia previa encontró que `model.request` puede esperar callbacks asíncronos y modificar headers, y que un UUID explícito puede viajar al request HTTP. También encontró que lifecycle y hook no comparten un `execution_id` nativo; los eventos llegan asincrónicamente, y `http.response` precede al EOF SSE. Los hijos tienen sesiones propias con `parentID`; foreground y background se comportan distinto.

LARM-004.2.4 validó en una PoC aislada un punto de extensión que sí permite crear y propagar IDs sin modificar OpenCode: un plugin envuelve `subagent.execute` mediante `ctx.tool.transform`; genera un UUID nuevo por invocación; intercepta `context.progress({sessionID})`, llamado después de crear la sesión hija y antes de `sessions.prompt`; y enlaza ahí la sesión al `execution_id` y al padre ya conocidos. El hook `model.request` lee ese vínculo explícito y añade `X-LARM-Execution-ID`, `X-LARM-Parent-Execution-ID` y un `X-LARM-Request-ID` nuevo. La identidad raíz se pasó desde un launcher dedicado y se enlazó en el hook `prompt` antes de sus solicitudes HTTP, incluida la primera de tipo `title`.

La instrumentación comprobó raíz, trabajos sucesivos en la misma sesión, foreground, background, hijos concurrentes y llamadas repetidas al mismo agente; también siguió request IDs durante SSE, interrupción y error. Véase [`LARM-004.2.4-subagent-identity.md`](../../evidence/LARM-004.2.4-subagent-identity.md). Esto es una PoC de cooperación wrapper/plugin, no una identidad nativa de OpenCode ni el adaptador definitivo. El enlace `sessionID → execution_id` solo es admisible porque se establece explícitamente antes del prompt; reconstruirlo desde eventos, nombre de agente o proximidad temporal sigue prohibido.

El UUID raíz por variable de entorno solo es seguro cuando el launcher dedica ese proceso a una ejecución. Un OpenCode server persistente que atienda varios trabajos debe recibir el ID por invocación (por ejemplo, metadata del `session.prompt` del wrapper y el hook `prompt`), nunca por variable global compartida. La PoC no valida esa variante de server compartido ni invocaciones superpuestas sobre una misma sesión hija reutilizada; deberán ser gates de implementación. En caso de no poder envolver el tool nativo o establecer el vínculo antes del prompt, no administrar leases por ejecución en esa rama.

**Precisión añadida por LARM-004.2.5:** en el servidor compartido v2.2.5, metadata explícita por cada `session.prompt` correlacionó wrappers en sesiones distintas y trabajos sucesivos en una sesión. No hizo seguro solapar dos prompts sobre la **misma** sesión: dos llamadas concurrentes fueron aceptadas y ambas enlazaron IDs en el hook `prompt`, pero el único `session.execution.started` y request HTTP observados usaron el último ID enlazado. Como `model.request` expone `sessionID`, no el token de invocación, una tabla mutable `sessionID → execution_id` pierde la identidad anterior. La integración debe serializar o rechazar prompts simultáneos por sesión, salvo que un contexto de invocación llegue explícitamente hasta cada `model.request`.

La misma limitación se observó con dos llamadas simultáneas de `subagent.execute` que reutilizaron una sesión hija: OpenCode aceptó ambas y el wrapper obtuvo dos IDs hijos, pero la solicitud HTTP observada en esa sesión solo pudo ligarse al último ID; en esa corrida los requests se serializaron (máximo un stream activo). Esa serialización observada no es una clave de correlación ni una garantía general. No administrar leases por ejecución para una sesión hija reutilizada concurrentemente sin token por invocación o una barrera que rechace/serialice esa condición. Véase [`LARM-004.2.5-shared-server-concurrency.md`](../../evidence/LARM-004.2.5-shared-server-concurrency.md).

**Precisión de LARM-004.2.5.1 (coordinador aislado):** cuando los hooks solo reciben `sessionID`, la barrera se adquiere antes de `session.prompt` para la raíz y, para un subagente, dentro del wrapper de `subagent.execute` en `context.progress({sessionID})`, antes de permitir el prompt hijo. Su autoridad no puede ser un mapa/mutex de un proceso plugin: wrappers del mismo servidor deben usar el mismo `server_id` canónico y el mismo coordinador local. La PoC de coordinador usa un journal atómico compartido y lock de archivo del sistema operativo; un `BUSY` se rechaza inmediatamente. Un registro `UNKNOWN` conserva la propiedad y no se reasigna por timeout, desconexión o reinicio. En LARM-004.2.5.1 aún no estaba conectado al plugin/launcher. La integración de LARM-004.2.5.2 y sus límites probados se describen abajo y en [`LARM-004.2.5.2-opencode-integration.md`](../../evidence/LARM-004.2.5.2-opencode-integration.md).

**Integración aislada LARM-004.2.5.2:** `integrations/opencode/launcher.py` registra y adquiere antes del primer `POST /api/session/{id}/prompt`; el hook raíz solo enlaza metadata si el snapshot confirma esa propiedad preexistente. El plugin envuelve el tool nativo `subagent`; su `progress({sessionID})` crea la ejecución hija y adquiere antes de dejar continuar al prompt hijo. Un `BUSY` rechaza la invocación y terminaliza/limpia únicamente el registro hijo CREATED, sin modificar el owner. Así se evita la sobrescritura por dos prompts raíz sobre una sesión o por dos llamadas que reutilizan una sesión hija. El `model.request` rechaza una sesión hija sin identidad ligada.

El plugin registra requests antes del transporte. `http.response` registra status y consume un clon del body solo para detectar `[DONE]` y EOF; no guarda payload. La señal lifecycle terminal se combina con requests terminales antes de `Finalize`/`Cleanup`. No se usa `session.idle`, el retorno del wrapper ni el terminal del padre como prueba de fin del hijo background. Si no se observa respuesta/EOF/error de transporte definitivo, la referencia queda activa y la sesión protegida. No se crean leases y el coordinador FIFO/scheduler LARM no participa.

**Integración de leases LARM-004.2.6a:** el launcher crea la ejecución y adquiere la barrera de sesión; registra en el coordinador el intento `(model_id, idempotency_key)` antes del `POST /api/v1/leases`; usa la misma clave UUIDv4 para retries de transporte; y no envía el prompt hasta leer `ACTIVE` para el modelo exacto. El lease ID se enlaza de forma inmutable a esa ejecución antes de `session.prompt`. Un restart del coordinador que encuentre ese intento sin lease ID lo convierte a `UNKNOWN` y conserva la barrera. El launcher fija también `{providerID, modelID}` en el prompt; el plugin comprueba que el provider gestionado es el LARM configurado.

El hook `model.request` comprueba modelo/provider, crea un Request ID por intento, comprueba el vínculo de lease del coordinador y propaga `X-LARM-Execution-ID`, `X-LARM-Request-ID` y `X-LARM-Lease-ID`. Solo el provider LARM recibe esos headers; los requests externos no administrados retornan del hook sin mutación. El proxy de LARM valida `ACTIVE` y modelo, pero **no** valida propiedad de lease por `execution_id`; esa asociación y el rechazo de lease ajena son garantías del launcher/plugin/coordinador local, no de la API LARM.

`http.request` marca el inicio de transporte; `http.response` registra headers/status y solo marca `body_started` al primer chunk. SSE se considera terminado tras EOF y `[DONE]`; un error/cancelación solo terminaliza el request si el transporte confirma error o el `Request.signal` está abortado. El lifecycle terminal hace `BeginFinalize`, que impide crear/iniciar requests nuevos sin liberar la sesión. Cuando todos los requests están terminales, el plugin detiene y espera el heartbeat, consulta/libera la lease y confirma un estado terminal de LARM; únicamente después llama `Finalize` y `Cleanup`. Un resultado ambiguo conserva la sesión y marca `UNKNOWN`; no hay readquisición/reconciliación automática. El heartbeat vive en el plugin del servidor OpenCode mientras la ejecución raíz está ligada.

Durante esta etapa el transform del tool nativo `subagent` falla de forma cerrada para ejecuciones administradas; si no puede instalarse el transform, el plugin no inicia. No se crean ejecuciones/leases hijas. Véase [`LARM-004.2.6a-managed-execution-leases.md`](../../evidence/LARM-004.2.6a-managed-execution-leases.md) para límites y pruebas. La corrida GGUF real hizo una inferencia (no varias) en una sola lease; por ello el mecanismo está implementado, pero no se declara PASS para el gate 004.2.6a.

`session.execution.*` puede servir como señal terminal solo cuando el bridge ha ligado inequívocamente esa ejecución al contexto explícito. No es por sí mismo un identificador retornable al hook.

## 6. Inferencias, release y errores

El adaptador registra request antes de enviarlo al proxy LARM. Considera activo todo request enviado que no haya alcanzado EOF, error de transporte definitivo o cancelación confirmada. Registra separadamente status HTTP, marcador SSE, EOF, cancelación, error y desconexión.

**No libera una lease con requests activos o `UNKNOWN`.** Una respuesta 200/headers no decrementa el contador. Una respuesta de error HTTP termina solo después de consumir/cerrar su body. Un cancel normal se confirma cuando el adaptador cierra/cancela la petición aguas arriba y observa su finalización; si se pierde la conexión antes de confirmarla, conserva `UNKNOWN` y no readquiere/libera como si nada hubiera ocurrido. LARM actualmente mantiene fijada la inferencia admitida aunque la lease se libere/expire; esto es defensa adicional, no razón para violar la regla conservadora del adaptador.

Ante fallo de backend con respuesta y EOF, marcar `FAILED` y decrementar. Ante conexión rechazada antes de envío, registrar fallo definitivo. Ante caída después de enviar sin respuesta inequívoca, `UNKNOWN`: no retry ciego, no release prematuro. Una cancelación de ejecución cancela solicitudes/esperas encoladas, espera confirmación de sus términos y después libera la lease.

## 7. Subagentes y cesión segura

Política uniforme: antes de despachar un hijo potencialmente inferente, el padre no debe tener requests activos ni lease retenida. El adaptador confirma `in_flight(parent)=0`, registra `YIELDED`, libera lease de padre y solo después permite al despachador iniciar el hijo. El hijo tiene ID y lease propios. El padre readquiere FIFO tras acabar el hijo/descendencia; la lease anterior no se “reanuda”.

Para background, el gate se mantiene hasta que **todos** los descendientes del padre sean terminales; el padre puede continuar trabajo no-modelo, pero sus model requests se esperan localmente. Esto es deliberadamente más restrictivo que la concurrencia que OpenCode permite: evita que un request del padre se cuele al FIFO por delante del hijo que necesita el runtime. Nunca se toma la creación de sesión como prueba de espera ni se libera por inactividad.

| Caso | Regla |
|---|---|
| Padre sin hijos | Reutiliza su lease activa para varias inferencias propias; libera al finalizar y cuando `in_flight=0`. |
| Hijo foreground | Padre cede antes del despacho, espera terminal del hijo; readquiere solo al necesitar modelo. |
| Hijo background | Padre cede; las solicitudes de modelo del padre quedan gated hasta el último descendiente terminal. |
| Hijos secuenciales | Cada hijo tiene ID/lease separados; no inicia el siguiente hasta completar/cancelar el anterior si así lo exige el tool. |
| Hijos concurrentes | IDs y estados separados. Sus adquisiciones se serializan por FIFO LARM; padre queda cedido mientras cualquiera siga activa. |
| Mismo modelo padre/hijo | IDs y leases siguen separados; que el proceso residente pueda reutilizarse no implica compartir ownership. |
| Modelos distintos | FIFO serializa el cambio. No hay lease del padre retenida mientras el hijo espera. |

**Criterio de cesión válida:** contexto del hijo creado y persistido; requests activos del padre en cero; ninguna adquisición padre encolada; lease del padre liberada/terminal confirmada (o estado desconocido bloqueado); y gate del padre instalado antes de despachar. Si no puede establecerse esta barrera atómicamente, no se administra lease por ejecución para esa rama: usar modo por inferencia o abortar el despacho.

## 8. Compatibilidad y contrato LARM actual

Contratos revisados en `README.md`, `internal/api/handler.go`, `internal/api/proxy.go` y `internal/scheduler/{scheduler.go,state.go,queue.go}`:

| Capacidad | Estado actual |
|---|---|
| `POST /api/v1/leases` con `{"model":"…"}` | `202`, asigna `lease_id`, encola FIFO. Acepta opcionalmente `Idempotency-Key` UUID; sin él conserva el flujo anterior. |
| `GET /api/v1/leases/{id}` | Estado lease y posición de cola. |
| `POST /api/v1/leases/{id}/heartbeat` | TTL renovable hasta `max_lease_duration`. Timeout del cliente implica resultado incierto. |
| `DELETE /api/v1/leases/{id}` | Cancela cola o libera lease; terminal idempotente en scheduler. Inferencias admitidas mantienen backend fijado hasta `EndInference`. |
| Proxy `/v1/chat/completions`, `/v1/embeddings` | Lease externa por `X-LARM-Lease-ID`, valida ACTIVE/modelo; modo sin header mantiene lease automática por inferencia. SSE se copia hasta EOF. |
| Scheduler | Worker único, una lease/modelo activo, FIFO global, varias referencias `BeginInference` por lease. |
| Recuperación | Cola y leases viven en memoria; shutdown cancela; restart no restaura ownership. El registry durable de idempotencia reconcilia operaciones no terminales como `RECOVERY_REQUIRED` (sin reactivar procesos/leases) y conserva terminales. No existe contador por lease público. |

El adaptador externo puede gestionar leases, heartbeat, headers y el contador por request sin cambiar el scheduler. Headers de ejecución/request son metadata ignorada hoy por LARM; no deben suponerse persistidos allí. La única excepción es `Idempotency-Key` en `POST /api/v1/leases`, especificado debajo. Otros clientes OpenAI-compatible sin plugin continúan con lease automática por llamada y no se les atribuye una ejecución falsa.

Para evitar duplicados durante operación normal, el adaptador serializa adquisición por `execution_id`, mantiene un solo intento pendiente y persiste la respuesta `lease_id` antes de usarla. La extensión LARM-005 a continuación permite reconciliar pérdida de respuesta en la misma instancia LARM. En un reinicio, un registro no terminal no permite recuperar el ownership: se mantiene reservado y devuelve `RECOVERY_REQUIRED` en vez de re-adquirir o reactivar el runtime.

### Idempotencia de adquisición LARM-005

`POST /api/v1/leases` acepta un header opcional `Idempotency-Key` con UUID textual `8-4-4-4-12` hexadecimal; no se exige versión/variante UUID, no se acepta vacío y se normaliza a minúsculas. La clave es única dentro de la instancia lógica de LARM. La solicitud canónica es el único parámetro actual `model`, serializado por `encoding/json` como objeto compacto `{"model":<string JSON exacto>}`; el fingerprint persistido es SHA-256 de esos bytes. Whitespace del body no importa, el modelo sí es case-sensitive. Toda ampliación futura de parámetros que afecte a la adquisición deberá incorporarse a esta representación.

La reserva de clave, fingerprint y lease ID se escribe en el store antes de hacer visible la lease en el scheduler; luego, bajo el mismo mutex FIFO, se agrega una entrada única. Dos requests concurrentes se serializan en esa transición. Si falla el store, no se acepta la adquisición. Los retries no tocan el TTL, heartbeat, carga de modelo ni posición FIFO.

`QUEUED`/`PREPARING` se recuperan con el mismo ID y estado/posición vigente; `ACTIVE` devuelve la misma lease. `COMPLETED`, `EXPIRED`, `CANCELLED` y `FAILED` son resultados terminales (`409`) con snapshot recuperable. Una clave diferente con parámetros distintos es conflicto (`409`); UUID inválido es `400`; error de persistencia es `500`; queue llena mantiene `429`. El retry de operación activa responde `200`, el de operación pendiente `202`.

El archivo `scheduler.idempotency_store` por defecto deriva de la ruta YAML (`<ruta-config>.idempotency.json`), se reemplaza atómicamente y se crea con modo `0600`; no requiere DB externa. El store registra `clean_shutdown`: la primera actividad de adquisición/start manual vuelve sucio el marcador antes de aceptar trabajo; solo el cierre tras detener runtime lo vuelve limpio. LARM no restaura cola, leases, heartbeat ni procesos llama-server. Al cargar, los registros persistidos no terminales se convierten en `RECOVERY_REQUIRED`; un marcador sucio también bloquea el scheduler aunque no queden registros no terminales, porque puede haber una lease legacy sin clave o un proceso huérfano tras el crash. El endpoint devuelve `503` con ID/snapshot histórico cuando existe y no inventa una lease nueva. Se bloquean adquisiciones nuevas (con o sin clave) y arranques manuales: un proceso llama-server puede sobrevivir como huérfano al crash de LARM, y no existe una regla verificada para adoptarlo/detenerlo. Se requiere intervención operativa antes de borrar/rotar el store y reiniciar; hacerlo pierde la deduplicación histórica. Un reinicio limpio conserva tombstones, permite retries terminales (`409`) y claves nuevas. La reserva se persiste antes de encolar: una caída en ese punto deja un registro no terminal que tras restart queda conservadoramente no recuperable; una caída luego de encolar pero antes de responder permite recuperar la misma operación mientras LARM siga en ejecución, y después de restart queda explícitamente no recuperable. Esta garantía es no duplicación, no exactly-once recuperable entre reinicios.

Los registros (incluidos tombstones terminales) se retienen indefinidamente para que limpiar no permita reutilización silenciosa. Borrar manualmente el archivo —solo con LARM detenido— reinicia el ámbito y elimina la garantía para claves previas; no compartirlo entre procesos independientes. No se limpian registros activos. Esto añade almacenamiento local proporcional a adquisiciones históricas.

Heartbeat solo mientras el owner esté vivo, no terminal, no cedido y la lease sea conocida; detener al ceder/finalizar. Un heartbeat fallido pasa a desconocido: consultar `GET` antes de decidir, no asumir expiración. Respetar TTL y duración máxima; nunca usar heartbeat para mantener un padre bloqueando al hijo.

## 9. Recuperación, seguridad y compatibilidad

El journal durable guarda IDs, relaciones padre/hijo, transiciones, request IDs, intención de lease, lease ID conocido, modelo, estado de heartbeat y marcas de terminal (no prompts, bodies ni credenciales). En restart, no inferir ejecución activa de una sesión abierta. Reconciliar solo con propietario/process handle y lease IDs conocidos; consultar GET/status, y mantener `RECOVERY_REQUIRED` si no se puede probar el resultado. Lease conocida sin owner vivo deja de recibir heartbeat y expira; liberar explícitamente solo al verificar requests activos en cero. Lease con creación ambigua no se vuelve a crear a ciegas.

Los UUID no son credenciales. La API de lease actual tampoco autentica el ID: servicio loopback y clientes locales confiables solamente, sin exponer headers a logs o a clientes no confiables. El adaptador valida tamaño/formato, vínculo padre-hijo, modelo, duplicados y estado terminal. Rechaza versión desconocida, ejecución ausente, header de ejecución contradictorio y request ID repetido con contenido distinto.

## 10. Etapas y gates de implementación

1. **Bridge de identidad:** demostrar root wrapper → plugin → request, dos roots concurrentes, retries, y despacho de hijos con IDs propios/contexto explícito. Si OpenCode no permite el contexto hijo, detener la variante de lease por ejecución.
2. **Adaptador HTTP aislado:** request registry, fin real de body/EOF, cancel/error/desconexión, journal y claves de idempotencia local; probar sin LARM real.
3. **Integración LARM controlada:** usar endpoints existentes, probar FIFO/heartbeat/release y A→B foreground/background con leases de prueba autorizadas. No liberar con requests activos. Primero instrumentar/reconciliar `active_inferences`.
4. **Recuperación/compatibilidad:** matar/reiniciar únicamente fixtures autorizadas del adaptador/LARM, probar estado ambiguo y TTL; verificar clientes sin plugin siguen en lease automática.
5. **Decisión de producto:** si se requiere reconciliación exactly-once tras crash entre POST y journal, autorizar y diseñar la mínima extensión idempotente LARM. No ampliar el scheduler antes de observar esta necesidad.

## Referencias del repositorio

- [`README.md`](../../README.md), “Scheduler y reservas” / “Proxy de inferencia OpenAI”.
- [`internal/api/handler.go`](../../internal/api/handler.go), [`internal/api/proxy.go`](../../internal/api/proxy.go).
- [`internal/scheduler/scheduler.go`](../../internal/scheduler/scheduler.go), [`state.go`](../../internal/scheduler/state.go), [`queue.go`](../../internal/scheduler/queue.go).
- Evidencia OpenCode: [`LARM-004.2.1`](../../evidence/LARM-004.2.1-opencode-agent-lifecycle.md), [`LARM-004.2.2a`](../../evidence/LARM-004.2.2a-opencode-managed-leases.md), [`LARM-004.2.2a.1`](../../evidence/LARM-004.2.2a.1-opencode-http-correlation.md).
