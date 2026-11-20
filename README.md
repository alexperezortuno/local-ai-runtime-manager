# Local AI Runtime Manager (LARM)

LARM es un servicio Go independiente de clientes e IDEs que registra modelos GGUF, administra procesos locales de `llama-server` bajo demanda y ofrece un proxy de inferencia compatible con los endpoints esenciales de OpenAI.

## Arquitectura

- `internal/config`: carga YAML, expansión de variables y validación de rutas, puertos y parámetros.
- `internal/models`: registro inmutable de modelos configurados.
- `internal/runtime`: supervisa cada proceso/grupo de procesos y conserva su identidad.
- `internal/scheduler`: worker único, cola FIFO global, leases y políticas de residencia.
- `internal/api/proxy.go`: proxy OpenAI para chat, SSE y embeddings; delega carga/descarga al scheduler.
- `internal/backend`: argumentos de `llama-server` y sondeo `/health`.
- `internal/api`: API HTTP con `net/http`.
- `cmd/larm`: carga configuración, escucha y apagado ordenado.

El listener HTTP local solo acepta direcciones IP loopback (`127.0.0.1` o `::1`). Un listener LAN nativo, explícito y separado puede habilitarse mediante `server.lan`; utiliza TLS, autenticación Bearer y una allowlist CIDR. Expone `GET /api/v1/lan/health` y las rutas OpenAI-compatible `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/responses` y `POST /v1/embeddings`. Las peticiones LAN reutilizan el mismo Scheduler, las mismas leases automáticas y el mismo proxy que las peticiones locales; el cliente LAN nunca accede directamente a `llama-server`.

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
    host: "127.0.0.1"
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
    keep_alive: false
    time_alive: "5m"
scheduler:
  max_active_models: 1
  max_queue_size: 100
  lease_ttl: "10m"
  heartbeat_interval: "30s"
  max_lease_duration: "2h"
  # Opcional; por defecto se usa <ruta-config>.idempotency.json
  # idempotency_store: "/var/lib/larm/leases.json"
```

Al iniciar se validan el binario ejecutable, los archivos, los IDs, puertos y parámetros. Cada modelo necesita un puerto propio, distinto del puerto de la API.

La referencia de los parámetros opcionales de `llama.cpp` está en
[`docs/larm-009-llamacpp-parameters.md`](docs/larm-009-llamacpp-parameters.md).

### Acceso HTTPS desde la LAN

El listener LAN está deshabilitado por defecto y es independiente del listener
local. Para habilitarlo se requieren TLS, Bearer y una allowlist CIDR. El
listener local y el LAN son dos superficies distintas:

```mermaid
flowchart LR
    A["OpenCode / Cliente Linux"] -->|"HTTPS + Bearer"| B["LARM LAN HTTPS"]
    B --> C["LARM Core"]
    C -->|"Loopback"| D["llama-server"]
```

- **API local de LARM:** `server.host` solo puede ser loopback y atiende por
  defecto en `127.0.0.1:8090`.
- **Listener HTTPS LAN:** `server.lan.host:server.lan.port` es una IP no
  loopback, con TLS, Bearer y ACL CIDR.
- **Backend:** cada `models.<id>.host` debe configurarse como `127.0.0.1`
  (si se omite, LARM usa ese valor por defecto). El código permite otros hosts
  de modelo por compatibilidad; esos valores no cumplen el aislamiento
  requerido.
- **Cliente remoto:** solo llama al endpoint HTTPS de LARM. Nunca debe llamar
  directamente al puerto de `llama-server`.

Las rutas LAN permitidas son exactamente
`GET /api/v1/lan/health`, `GET /v1/models`, `POST /v1/chat/completions`,
`POST /v1/responses` y `POST /v1/embeddings`. Las rutas administrativas y las
variantes de método o ruta responden `404`. El Bearer autentica ante LARM y no
se reenvía a `llama-server`.

### Certificados TLS: CA privada y certificado del servidor

El siguiente procedimiento crea una CA privada y un certificado de servidor
firmado por ella. Sustituye la IP y el DNS por los nombres que realmente usen
los clientes; esos valores deben aparecer en `subjectAltName`.

```bash
# Ejecutar en el servidor LARM. No uses el repositorio como almacén de claves.
umask 077
export LARM_LAN_IP="192.168.1.20"
export LARM_LAN_DNS="larm.lan.internal"
export LARM_TLS_DIR="${LARM_TLS_DIR:-$HOME/.config/larm/tls}"
install -d -m 700 "$LARM_TLS_DIR"
cd "$LARM_TLS_DIR"

# 1. Clave privada y certificado público de la CA.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out larm-ca-key.pem
chmod 600 larm-ca-key.pem
cat > ca.cnf <<'EOF'
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no
[req_distinguished_name]
CN = LARM Private CA
[v3_ca]
basicConstraints = critical, CA:true, pathlen:1
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EOF
openssl req -x509 -new -sha256 -days 3650 \
  -key larm-ca-key.pem -out larm-ca-cert.crt -config ca.cnf
chmod 644 larm-ca-cert.crt

# 2. Clave privada y CSR del listener HTTPS.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out lan-key.pem
chmod 600 lan-key.pem
openssl req -new -sha256 -key lan-key.pem -out lan.csr \
  -subj "/CN=$LARM_LAN_IP"

# 3. Certificado de servidor con SAN para IP y DNS.
cat > lan-ext.cnf <<EOF
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = IP:$LARM_LAN_IP,DNS:$LARM_LAN_DNS
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
EOF
openssl x509 -req -sha256 -days 825 \
  -in lan.csr -CA larm-ca-cert.crt -CAkey larm-ca-key.pem \
  -CAcreateserial -out lan-cert.pem -extfile lan-ext.cnf
chmod 644 lan-cert.pem

# 4. Verificación de clave, SAN y cadena.
openssl pkey -in lan-key.pem -check -noout
openssl x509 -in lan-cert.pem -noout -subject -issuer -dates
openssl x509 -in lan-cert.pem -noout -ext subjectAltName
openssl verify -CAfile larm-ca-cert.crt lan-cert.pem
```

La **clave privada de la CA** (`larm-ca-key.pem`) firma certificados y nunca
se distribuye. El **certificado público de la CA** (`larm-ca-cert.crt`) solo
contiene la parte pública y sí se distribuye a los clientes. La **clave
privada del servidor** (`lan-key.pem`) permite al listener demostrar su
identidad y permanece únicamente en el servidor. El **certificado del
servidor** (`lan-cert.pem`) es público, contiene el SAN y se carga junto con
la clave privada por LARM.

Conserva las claves privadas con modo `0600`, los certificados públicos con
modo `0644` y el directorio privado con modo `0700`. No añadas ningún `.pem`,
`.crt`, `.csr`, `.cnf` o `.srl` generado al repositorio ni registres sus
contenidos. Renueva el certificado de servidor antes de los 825 días; si la
CA sigue vigente no es necesario redistribuirla. Si se compromete la clave de
la CA, crea una CA nueva y redistribuye su certificado.

### Credenciales Bearer

`LARM_LAN_API_KEYS` es una lista estática de credenciales separadas por coma.
LARM calcula sus hashes SHA-256 en memoria y valida
`Authorization: Bearer <credencial>` en tiempo constante. No hay usuarios,
login, refresh tokens ni endpoint para generar claves.

Genera una credencial sin escribirla literalmente en el historial:

```bash
LARM_LAN_API_KEYS="$(openssl rand -hex 32)"
export LARM_LAN_API_KEYS
test -n "${LARM_LAN_API_KEYS:-}" && echo "Credencial cargada"
```

Para rotación gradual se pueden cargar varias credenciales:

```bash
LARM_LAN_API_KEYS="${LARM_LAN_KEY_OLD},${LARM_LAN_KEY_NEW}"
export LARM_LAN_API_KEYS
```

Cada elemento se recorta y no puede estar vacío; no uses comas dentro de una
credencial. La variable debe existir antes de iniciar LARM. Cambiarla no
recarga las claves: para renovar o revocar, sustituye la lista y reinicia LARM
de forma controlada. No imprimas la variable, no la pongas en una URL, no la
envíes a logs y no la confirmes en Git.

### Configuración del servidor

Este YAML es ilustrativo y sintácticamente completo; no cambia los valores
predeterminados del proyecto ni es una configuración de producción:

```yaml
server:
  host: "127.0.0.1"
  port: 8090
  lan:
    enabled: true
    host: "192.168.1.20"
    port: 8443
    auth:
      type: "bearer"
      api_keys_env: "LARM_LAN_API_KEYS"
    access:
      allow:
        - "192.168.1.0/24"
    tls:
      cert_file: "${LARM_TLS_DIR}/lan-cert.pem"
      key_file: "${LARM_TLS_DIR}/lan-key.pem"
runtime:
  backend: "llamacpp"
  binary: "/usr/local/bin/llama-server"
  startup_timeout: "120s"
  shutdown_timeout: "15s"
models:
  code-agent:
    path: "/models/code-agent.gguf"
    host: "127.0.0.1"
    port: 18070
    context: 32768
    batch: 1024
    ubatch: 256
    gpu_layers: "all"
    parallel: 1
```

Los campos efectivos son `server.host`/`port` (API local),
`server.lan.enabled`/`host`/`port`, `server.lan.auth.type` (debe ser
`bearer`), `server.lan.auth.api_keys_env`,
`server.lan.access.allow`, `server.lan.tls.cert_file` y `key_file`.
`cert_file` y `key_file` deben ser rutas absolutas y formar un par TLS válido.
En este ejemplo `${LARM_TLS_DIR}` se expande antes de validar el YAML; debe
estar definida en el entorno que inicia LARM. Al cargar la configuración, LARM
comprueba que ambos archivos existan y que `tls.LoadX509KeyPair` pueda cargar
el par; si falla, no inicia el listener LAN.
`server.lan.host` no puede ser loopback ni unspecified. El backend usa
`models.<id>.host` y `port`; para aislamiento se debe usar `127.0.0.1`.

Arranque ilustrativo, con la credencial ya cargada en esa misma sesión:

```bash
export LARM_CONFIG="/srv/larm/models-lan.yaml"
./bin/larm -config "$LARM_CONFIG"
```

LARM también admite `LARM_CONFIG`, `LARM_HOST`, `LARM_PORT` y `LARM_BINARY`;
estos overrides no sustituyen la configuración `server.lan`. El cargador
expande `${NOMBRE}` dentro del YAML, pero las credenciales no se admiten
directamente en YAML.

### Distribución segura a macOS/Linux

Distribuye únicamente `larm-ca-cert.crt` y una credencial Bearer individual,
por un canal autenticado y cifrado (por ejemplo, una sesión SSH con
verificación de la huella del host o un gestor de secretos). No copies
`larm-ca-key.pem` ni `lan-key.pem` a los clientes.

En el cliente:

```bash
umask 077
mkdir -p "$HOME/certs"
chmod 700 "$HOME/certs"
# Copiar larm-ca-cert.crt por el canal seguro acordado. Sustituye la ruta
# remota por el valor real de LARM_TLS_DIR en el servidor:
scp operador@larm-host:/ruta/real/larm-ca-cert.crt "$HOME/certs/"
chmod 644 "$HOME/certs/larm-ca-cert.crt"
openssl dgst -sha256 "$HOME/certs/larm-ca-cert.crt"
```

Compara ese digest, fuera de banda, con el digest calculado en el servidor:

```bash
openssl dgst -sha256 "$LARM_TLS_DIR/larm-ca-cert.crt"
```

Un cliente puede probar la cadena sin desactivar TLS:

```bash
curl --cacert "$HOME/certs/larm-ca-cert.crt" \
  -H "Authorization: Bearer ${LARM_LAN_API_KEY}" \
  "https://192.168.1.20:8443/api/v1/lan/health"
```

`LARM_LAN_API_KEY` es una variable auxiliar del cliente y debe contener una
sola credencial que esté incluida en `LARM_LAN_API_KEYS` del servidor; LARM no
lee esa variable. Para cargarla sin dejar el valor en el historial, en bash:

```bash
read -r -s -p "LARM API key: " LARM_LAN_API_KEY
echo
export LARM_LAN_API_KEY
```

En zsh (habitual en macOS):

```zsh
read -r -s "LARM_LAN_API_KEY?LARM API key: "
print
export LARM_LAN_API_KEY
```

Para persistir TLS en **bash** (`~/.bashrc`) o **zsh** (`~/.zshrc`), añade:

```bash
export NODE_USE_SYSTEM_CA=1
export NODE_EXTRA_CA_CERTS="$HOME/certs/larm-ca-cert.crt"
export SSL_CERT_FILE="$HOME/certs/larm-ca-cert.crt"
```

Después ejecuta `source ~/.bashrc` o `source ~/.zshrc`. Estas variables son
para clientes Node.js/OpenCode cuando correspondan; no son variables de
configuración de LARM. No es recomendable escribir el Bearer directamente en
el archivo de shell. Si se necesita persistencia, usa un archivo separado
`$HOME/.config/larm/larm-api-key` con modo `0600`, un gestor de secretos o el
almacén seguro del sistema, y cárgalo antes de iniciar OpenCode. Un archivo
con la credencial es texto recuperable: no lo compartas ni lo incluyas en
Git.

Si eliges el archivo local, créalo sin poner el valor en el historial y
configura solo la lectura desde `~/.bashrc` o `~/.zshrc`:

```bash
umask 077
mkdir -p "$HOME/.config/larm"
read -r -s -p "LARM API key: " LARM_LAN_API_KEY
echo
printf '%s\n' "$LARM_LAN_API_KEY" > "$HOME/.config/larm/larm-api-key"
chmod 600 "$HOME/.config/larm/larm-api-key"
```

```bash
export LARM_LAN_API_KEY="$(cat "$HOME/.config/larm/larm-api-key")"
```

### OpenCode remoto

Para OpenCode v2, configura `opencode.jsonc` con el provider `larm`, el SDK
OpenAI-compatible y el modelo registrado por LARM como `code-agent`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "model": "larm/code-agent",
  "providers": {
    "larm": {
      "name": "LARM LAN",
      "env": ["LARM_LAN_API_KEY"],
      "package": "aisdk:@ai-sdk/openai-compatible",
      "settings": {
        "baseURL": "https://192.168.1.20:8443/v1",
        "apiKey": "{env:LARM_LAN_API_KEY}"
      },
      "models": {
        "code-agent": {
          "modelID": "code-agent",
          "name": "LARM code-agent",
          "capabilities": {
            "tools": true,
            "input": ["text"],
            "output": ["text"]
          },
          "limit": {
            "context": 32768,
            "output": 4096
          }
        }
      }
    }
  }
}
```

La IP del ejemplo (`192.168.1.20`) y la red `192.168.1.0/24` son de una red
de ejemplo: deben adaptarse al entorno real y coincidir con el SAN y la ACL.
No uses otro identificador de modelo en la solicitud: `code-agent` debe
coincidir con la clave de `models` de LARM.

Comprueba la configuración efectiva sin mostrar secretos:

```bash
opencode debug config
opencode models
```

Verifica que aparezcan el provider `larm`, el endpoint `/v1` y
`larm/code-agent`. Si el servicio persistente de OpenCode ya estaba iniciado,
el entorno de la terminal no se hereda retroactivamente. Una forma soportada
de cargar el entorno persistente es:

```bash
opencode service set env LARM_LAN_API_KEY "$LARM_LAN_API_KEY"
opencode service set env NODE_USE_SYSTEM_CA 1
opencode service set env NODE_EXTRA_CA_CERTS "$HOME/certs/larm-ca-cert.crt"
opencode service set env SSL_CERT_FILE "$HOME/certs/larm-ca-cert.crt"
opencode service restart
opencode service status
```

El valor expandido no queda escrito literalmente en el historial, pero el
servicio debe conservarlo para poder iniciar; trátalo como un secreto y
protege el almacenamiento del servicio. Si no aceptas esa persistencia, carga
la credencial solo en un proceso `--standalone` o mediante un gestor de
secretos compatible con tu instalación.

Una inferencia de comprobación desde el cliente es:

```bash
opencode run --model larm/code-agent "Responde únicamente: LARM OK"
```

No es necesario `--standalone` si el servicio persistente ya tiene el
endpoint, la credencial y las variables TLS. `--standalone` solo usa el
entorno de la sesión que lo inicia y sirve para aislar una prueba.

### Pruebas de conectividad y seguridad

Estas pruebas son procedimientos reproducibles; no implican que se hayan
ejecutado en este checkout. Define solo variables auxiliares del cliente:

```bash
export LARM_URL="https://192.168.1.20:8443"
export LARM_CA_FILE="$HOME/certs/larm-ca-cert.crt"
```

**TEST-01 — HTTPS autenticado (200):**

```bash
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_API_KEY" \
  -o /dev/null -w 'HTTP %{http_code}\n' \
  "$LARM_URL/v1/models"
```

**TEST-02 — Inferencia real (200):**

```bash
curl --cacert "$LARM_CA_FILE" \
  -H "Authorization: Bearer $LARM_LAN_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","messages":[{"role":"user","content":"Responde: OK"}]}' \
  -o /tmp/larm-chat.json -w 'HTTP %{http_code}\n' \
  "$LARM_URL/v1/chat/completions"
```

La respuesta debe ser JSON de Chat Completions con una elección; el modelo
debe aparecer previamente en `GET /v1/models`.

**TEST-03 — Sin autenticación (401):**

```bash
curl --cacert "$LARM_CA_FILE" -o /dev/null -w 'HTTP %{http_code}\n' \
  "$LARM_URL/v1/models"
```

**TEST-04 — Credencial incorrecta (401):**

```bash
curl --cacert "$LARM_CA_FILE" \
  -H 'Authorization: Bearer invalid-token-for-test' \
  -o /dev/null -w 'HTTP %{http_code}\n' \
  "$LARM_URL/v1/models"
```

**TEST-05 — CA no confiable:** usa un archivo CA distinto al de LARM:

```bash
curl --cacert "$HOME/certs/otra-ca.crt" "$LARM_URL/v1/models"
```

Debe fallar la verificación TLS (`curl` puede mostrar
`SSL certificate problem`); no uses `-k` ni `--insecure` para convertirlo en
éxito.

**TEST-06 — Backend aislado:** desde el cliente remoto, prueba el puerto
loopback que figura en `models.<id>.port`:

```bash
nc -vz 192.168.1.20 18070
```

Debe fallar o no ser enrutable. En el servidor LARM comprueba que el proceso
escucha en loopback, por ejemplo con `lsof -nP -iTCP:18070 -sTCP:LISTEN` en
macOS o `ss -ltn` en Linux. Si escucha en una IP LAN, corrige
`models.code-agent.host` a `127.0.0.1` antes de declarar aislamiento.

**TEST-07 — OpenCode:** ejecuta el comando `opencode run` anterior y confirma
una respuesta real, no solo que el provider aparezca en el catálogo.

**TEST-08 — ACL:** con un cliente fuera de `server.lan.access.allow`, una
petición con Bearer válido debe devolver `403`. Una petición sin Bearer se
rechaza antes por `401`; esto es el orden real de la implementación.

### Diagnóstico

- **`UNABLE_TO_VERIFY_LEAF_SIGNATURE`:** la CA del cliente no corresponde a
  la que firmó `lan-cert.pem`, el PEM está incompleto o el servicio cargó
  otro certificado. Repite `openssl dgst`, `openssl verify -CAfile ...` y el
  `curl --cacert` del TEST-01.
- **SAN ausente o incorrecto:** el nombre/IP de la URL no coincide con
  `subjectAltName`; el CN no lo sustituye. Regenera el certificado con la IP
  y/o DNS exactos y renueva solo el certificado de servidor.
- **HTTP 401:** falta `Authorization`, el Bearer no coincide, se envió la
  lista completa separada por comas en vez de una clave individual, o LARM se
  inició antes de cargar `LARM_LAN_API_KEYS`. No pongas el token en la URL ni
  lo imprimas.
- **HTTP 403:** la credencial es válida pero la IP observada no pertenece a
  ningún CIDR de `server.lan.access.allow`. Revisa NAT, proxy y la red real;
  cambiar la ACL requiere reiniciar LARM.
- **HTTP 503 / scheduler / recovery:** puede ser backend no disponible,
  timeout de cola, capacidad, identidad de proceso no verificada o
  `RECOVERY_REQUIRED`. Revisa `larm status`, el log y el estado local del
  servidor; la superficie LAN no expone la administración.
- **Modelo no registrado / 404:** usa exactamente un ID devuelto por
  `GET /v1/models`, por ejemplo `code-agent`; los alias o nombres del archivo
  GGUF no son IDs de solicitud si no están registrados.
- **OpenCode funciona en CLI pero no en su servicio persistente:** el servicio
  tiene un entorno distinto. Comprueba `opencode service status`, carga las
  variables en el entorno del servicio, ejecuta `opencode service restart` y
  revisa `opencode debug config`.
- **Variables no heredadas:** exportarlas en una terminal no cambia un proceso
  ya iniciado ni una aplicación gráfica. Configúralas antes del arranque o
  mediante el mecanismo de entorno del servicio. `NODE_EXTRA_CA_CERTS` se
  evalúa al iniciar Node.
- **Puerto HTTPS inaccesible:** confirma que `server.lan.host` sea la IP
  correcta, que el puerto no esté bloqueado por firewall y que el cliente esté
  en un CIDR permitido. `server.host` local no debe cambiarse a una IP LAN.
- **`llama-server` expuesto incorrectamente:** el listener del backend debe
  aparecer en `127.0.0.1:PORT` (o loopback equivalente), nunca en una IP LAN.
  Corrige `models.*.host`; no intentes proteger un backend expuesto usando el
  Bearer del listener LAN.

La referencia contractual ampliada está en
[`docs/larm-008.2-remote-model-inference.md`](docs/larm-008.2-remote-model-inference.md)
y [`docs/larm-008.5-backend-loopback-isolation.md`](docs/larm-008.5-backend-loopback-isolation.md).


## CLI operativa

Con LARM ejecutándose en la URL por defecto (`http://127.0.0.1:8090`), las
consultas de uso diario son de solo lectura y reutilizan los snapshots de la
API:

```bash
larm models       # estado, residencia, PID/ProcessInstanceID, leases y capacidad
larm status       # servicio, modelos, residentes, capacidad, cola y Recovery
larm queue        # FIFO, estado, lease_id y espera observada
larm capacity code-fast # plan de memoria dry-run, sólo lectura
larm doctor       # validación read-only; no carga modelos ni ejecuta Recovery
```

Se puede indicar `-url URL` para las consultas y `-config PATH` para `doctor`.
Un servicio detenido se informa como `WARN`, no como corrupción. `doctor` no
crea archivos, claves ni capabilities.

## API

```bash
curl http://127.0.0.1:8090/health
curl http://127.0.0.1:8090/api/v1/models
curl http://127.0.0.1:8090/api/v1/status
curl http://127.0.0.1:8090/api/v1/processes
curl http://127.0.0.1:8090/api/v1/capacity/code-fast
curl 'http://127.0.0.1:8090/api/v1/capacity?model=code-fast'
curl -i -X POST http://127.0.0.1:8090/api/v1/models/code-fast/start # rechazado: requiere socket protegido
```

Las operaciones de control esperan a que el backend esté listo o a que termine el apagado. Errores de modelo inexistente, conflicto y backend no disponible responden con `404`, `409` y `503`, respectivamente.

`GET /api/v1/processes` y `GET /api/v1/processes/{process_instance_id}` son consultas read-only de identidad, ownership, grupo, evidencia y terminación. LARM no considera `SIGTERM`, `SIGKILL`, `Wait()` ni un puerto cerrado como prueba de terminación: si el grupo o descendientes no pueden verificarse, conserva `TERMINATION_UNKNOWN` y bloquea el cambio de modelo.

`GET /api/v1/models` no devuelve el campo interno `path`: los clientes no
privilegiados reciben sólo metadatos operativos y no necesitan conocer la ruta
absoluta del GGUF. La configuración y Runtime conservan la ruta para cargar el
modelo. La CLI operativa mantiene la misma salida sin rutas internas.

### Administración autorizada

La descarga administrativa no está disponible por HTTP. Requiere el socket Unix,
peer credentials y una capability Ed25519 dirigida al modelo:

```bash
larm admin -socket "$LARM_ADMIN_SOCKET" -private-key "$LARM_ADMIN_PRIVATE_KEY" \
  -out /tmp/code-agent.stop.cap auth issue-model-stop code-agent
larm admin -socket "$LARM_ADMIN_SOCKET" \
  -capability-file /tmp/code-agent.stop.cap model stop code-agent
```

El primer comando emite una capability y el segundo ejecuta el Stop verificado;
la autorización conserva scope, expiración, replay y auditoría.

### Scheduler y reservas

Un único worker ejecuta todas las transiciones automáticas del runtime. Las solicitudes se agregan al final de una cola FIFO global; las solicitudes en espera no dependen de que siga abierta la conexión HTTP que las creó. Se admite como máximo una lease activa y un modelo residente. Si el siguiente lease pide el modelo ya cargado, reutiliza el proceso; en caso contrario, LARM confirma su detención antes de iniciar el siguiente.

El ciclo de una reserva es `QUEUED → PREPARING → ACTIVE → COMPLETED` (liberación), o `FAILED`, `CANCELLED` o `EXPIRED`. El TTL empieza al activarse. El heartbeat lo extiende por `lease_ttl` sin superar `max_lease_duration`, que se cuenta desde la activación. La descarga y el siguiente cambio de modelo esperan hasta que terminen las inferencias admitidas, incluso si la lease se libera o vence durante ellas. La cola está solo en memoria y se pierde al reiniciar.

```bash
# Solicitar; HTTP 202 y un lease_id para consultar aunque el modelo siga cargando
curl -X POST http://127.0.0.1:8090/api/v1/leases \
  -H 'Content-Type: application/json' -d '{"model":"code-agent"}'

# Opcional: reintentar el mismo POST de forma idempotente
curl -X POST http://127.0.0.1:8090/api/v1/leases \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 2ef8d7ef-b335-4404-b72c-5b2c08a36101' \
  -d '{"model":"code-agent"}'

curl http://127.0.0.1:8090/api/v1/leases/LEASE_ID
curl -X POST http://127.0.0.1:8090/api/v1/leases/LEASE_ID/heartbeat
curl -X DELETE http://127.0.0.1:8090/api/v1/leases/LEASE_ID
curl http://127.0.0.1:8090/api/v1/queue
curl http://127.0.0.1:8090/api/v1/status
```

`GET /api/v1/leases/{id}` incluye `queue_position` (1-based; cero si no está encolada). `GET /api/v1/queue` y `/status` informan la reserva activa, la que está preparando, la cola pendiente y los modelos residentes con su política, PID y `ProcessInstanceID`. Una cola llena responde `429`; IDs/modelos desconocidos, `404`; heartbeat no aplicable o lease expirado, `409`.

`POST /api/v1/leases` acepta opcionalmente `Idempotency-Key`, en UUID textual `8-4-4-4-12` hexadecimal (no vacío; mayúsculas se normalizan). Sin el header conserva el comportamiento previo. Una clave nueva crea un registro durable antes de añadir una sola lease a FIFO. Un retry con la misma solicitud devuelve la lease/posición original (`202` en `QUEUED`/`PREPARING`, `200` en `ACTIVE`); no renueva TTL ni heartbeat. La única solicitud actual es `model`: su huella es SHA-256 de JSON compacto canónico `{"model":<valor JSON del model decodificado>}`; el valor de modelo es exacto y sensible a mayúsculas. La misma clave con otro modelo devuelve `409`.

Una clave inválida devuelve `400`; un retry de una operación terminal (`COMPLETED`, `EXPIRED`, `CANCELLED` o `FAILED`) devuelve `409` con el estado y snapshot original y nunca recrea la lease. El store registra actividad sucia antes de admitir adquisiciones/arranques y marca apagado limpio solo después de detener el runtime. Si LARM reinicia con estado sucio u operaciones persistidas, no restaura ownership ni relanza `llama-server`: conserva IDs históricos y responde `503` (`RECOVERY_REQUIRED`). La única excepción es un store sucio completamente vacío (sin adquisiciones ni reconciliaciones): Recovery solo lo marca limpio después de confirmar journal/admission normales y que Runtime no conserva procesos ni transiciones; el cambio también debe persistir de forma durable. Mientras exista cualquier otra recuperación, bloquea nuevas adquisiciones (con y sin clave) y arranques manuales para no cargar un runtime con ownership desconocido. No se adopta ni detiene automáticamente el proceso que sobreviviera a la caída. Un reinicio limpio conserva tombstones terminales y permite claves nuevas. El archivo se guarda atómicamente con permisos `0600`; por defecto está junto al archivo YAML como `<ruta-config>.idempotency.json`, o se configura con `scheduler.idempotency_store`. Las claves/tombstones se retienen indefinidamente (sin limpieza automática); eliminar el archivo reinicia el ámbito de idempotencia, por lo que se debe hacer solo con LARM detenido, tras resolver procesos huérfanos y sin reutilizar claves anteriores. Un archivo no debe compartirse entre procesos LARM independientes.

Los endpoints manuales de LARM-001 se conservan y usan el mismo worker. Solo se aceptan cuando no hay leases activas, solicitudes en preparación ni cola pendiente; de otro modo responden `409`. Así no pueden adelantarse a la cola ni detener un modelo reservado. Una operación manual ya aceptada se procesa antes de leases que lleguen después.

La configuración `scheduler` admite `max_active_models` (positivo), `max_queue_size` (por defecto `100`), `lease_ttl` (`10m`), `heartbeat_interval` (`30s`) y `max_lease_duration` (`2h`). Los tres tiempos deben ser positivos; heartbeat debe ser menor que TTL y la duración máxima al menos igual al TTL.

Cada modelo admite `keep_alive` (por defecto `false`) y `time_alive` (por defecto `"0s"`). `keep_alive: true` precarga el modelo automáticamente en segundo plano después de Recovery, mantiene el proceso cargado después de liberar la lease y tiene precedencia sobre `time_alive`. `time_alive > 0` también precarga y comienza la retención cuando el modelo queda READY y ocioso; al expirar no vuelve a precargar automáticamente. Un stop administrativo suprime la precarga hasta una nueva solicitud válida o un nuevo arranque. Con ambos valores por defecto se conserva la carga bajo demanda. Las duraciones deben ser válidas para `time.Duration` y no negativas. Lease, residencia y Recovery son responsabilidades independientes.

Un fallo de arranque marca la lease `FAILED` y el worker continúa. Una caída inesperada también falla la lease activa; el siguiente trabajo se procesa sin adoptar procesos externos. Al apagar LARM, las leases activas y pendientes se marcan `CANCELLED`, se cancelan cargas e inferencias en curso y se detiene el backend después de que las referencias de inferencia finalicen.



### Proxy de inferencia OpenAI

El proxy expone únicamente `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/responses` y `POST /v1/embeddings`. No permite elegir URL/puerto del backend: el puerto se resuelve desde el registro de modelos configurados y las solicitudes pasan por el scheduler FIFO.

`POST /v1/responses` implementa la OpenAI Responses API que consume Codex CLI. El proxy es transparente: reenvía el cuerpo de la petición y los eventos SSE sin transformarlos, de modo que el contrato lo define `llama-server`. La única diferencia con Chat Completions es el terminador del stream: la Responses API termina con el evento `response.completed` y no usa el centinela `data: [DONE]`. Codex CLI requiere `wire_api = "responses"`; los perfiles de referencia están en `configs/codex/` y el contrato completo en `docs/larm-008.3-codex-responses-api.md`.

En modo automático (sin header de lease), cada inferencia crea una lease, espera su activación y la libera al finalizar. El modelo permanece protegido durante toda la llamada, incluida la transmisión SSE. Ejemplos:

```bash
curl http://127.0.0.1:8090/v1/models

curl http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","messages":[{"role":"user","content":"Explica qué es un mutex en Go"}]}'

curl -N http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","stream":true,"messages":[{"role":"user","content":"Hola"}]}'

curl http://127.0.0.1:8090/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"code-agent","input":"texto para vectorizar"}'
```

Para reutilizar una lease externa, solicitarla con `POST /api/v1/leases`, esperar a que su estado sea `ACTIVE` y enviar su ID en `X-LARM-Lease-ID` en cada llamada. Por ejemplo:

```bash
curl http://127.0.0.1:8090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-LARM-Lease-ID: LEASE_ID' \
  -d '{"model":"code-agent","messages":[{"role":"user","content":"Siguiente pregunta"}]}'
```

La lease debe existir, estar activa y corresponder al modelo del JSON. El proxy no la libera. Si vence durante una inferencia, no admite llamadas nuevas, deja terminar la ya admitida y después libera el runtime. El ID no es una credencial: esta modalidad solo es segura para clientes locales confiables y no proporciona autorización multiusuario.

`proxy.queue_wait_timeout` (por defecto `5m`) solo limita la espera FIFO; `inference_timeout` (`10m`) limita cada inferencia; `backend_connect_timeout` (`5s`) limita la conexión al backend, y `stream_idle_timeout` (`2m`) limita el tiempo sin fragmentos SSE. Desconectar/cancelar una petición en cola elimina su lease automática; durante inferencia cancela el HTTP al backend y libera la referencia cuando este termina. El apagado cancela inferencias admitidas y espera su cierre antes de detener el proceso.

`GET /api/v1/status` conserva el estado de runtime y scheduler y añade `queued_requests`, `active_inferences`, `completed_requests` y `failed_requests`. No registra ni conserva prompts o respuestas.

Si `max_active_models` está ocupado por un residente persistente, una inferencia
para otro modelo puede responder `409` con un diagnóstico como
`model capacity exhausted: 1/1 resident slots occupied`; no se desaloja el
residente automáticamente.

El proxy reenvía las respuestas JSON y los eventos SSE de `llama-server`, incluidos campos presentes como `usage`, `finish_reason` y `tool_calls`; no implementa adaptaciones específicas por modelo.

## Estados

`STOPPED → STARTING → READY`, con `FAILED` ante un fallo de inicio o salida inesperada; `READY → STOPPING → STOPPED` al detener. `max_active_models` limita estrictamente los procesos residentes. Un `Start` repetido para el mismo modelo listo es idempotente.

## Limitaciones actuales

- Solo implementa las rutas OpenAI-compatible listadas (`/v1/models`,
  `/v1/chat/completions`, `/v1/responses` y `/v1/embeddings`); no incluye
  audio, imágenes ni otros endpoints OpenAI.
- No incluye prioridades, persistencia de cola, gestor de memoria RAM/VRAM ni autenticación multiusuario del listener local.
- El listener LAN publica health e inferencia autenticadas, pero no expone
  administración, procesos, configuración, leases ni journal.
- El aislamiento del backend depende de configurar cada
  `models.<id>.host` en loopback; LARM permite otros hosts de modelo por
  compatibilidad y no los corrige silenciosamente.
- Solo administra procesos `llama-server` iniciados por LARM; no adopta procesos externos.
- No valida que el modelo pueda cargarse en la memoria/GPU disponible; readiness lo determina el endpoint `/health`.
- Los argumentos avanzados dependen de la versión/compilación de `llama-server` instalada.
