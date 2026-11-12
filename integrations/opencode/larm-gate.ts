import { appendFileSync } from "node:fs"
import { randomUUID } from "node:crypto"
import { Plugin } from "@opencode/plugin"

type ExecutionRef = {
  executionID: string
  sessionID: string
  modelID?: string
  parentExecutionID?: string
  disposed?: boolean
}

type RequestRef = {
  executionID: string
  requestID: string
  sessionID: string
  modelID: string
  stream: boolean
  abortSignaled: boolean
  terminal?: string
}

const evidencePath = process.env.LARM_OPENCODE_EVIDENCE
const providerID = process.env.LARM_OPENCODE_PROVIDER_ID
if (!providerID) throw new Error("LARM provider is required")

// These maps are adapter observations only. LARM owns durable executions,
// inferences, leases, model residence, processes and recovery.
const executions = new Map<string, ExecutionRef>()
const requests = new WeakMap<Request, RequestRef>()
const activeRequests = new Map<string, RequestRef>()

function emit(type: string, fields: Record<string, unknown> = {}) {
  if (!evidencePath) return
  try {
    appendFileSync(evidencePath, JSON.stringify({ type, at: new Date().toISOString(), ...fields }) + "\n", { mode: 0o600 })
  } catch {}
}

function validID(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)
}

function executionFor(sessionID: string, modelID?: string, parentExecutionID?: string): ExecutionRef {
  const current = executions.get(sessionID)
  if (current && !current.disposed && (!modelID || !current.modelID || current.modelID === modelID)) {
    return current
  }
  const ref: ExecutionRef = { executionID: randomUUID(), sessionID, modelID, parentExecutionID }
  if (current) emit("execution_rotated", { session_id: sessionID, previous_execution_id: current.executionID, execution_id: ref.executionID, model_id: modelID })
  executions.set(sessionID, ref)
  emit("execution_declared", { execution_id: ref.executionID, session_id: sessionID, parent_execution_id: parentExecutionID, model_id: modelID, ownership: "DECLARED_UNVERIFIED" })
  return ref
}

function bindPrompt(sessionID: string, metadata: Record<string, unknown> | undefined) {
  const managed = metadata?.larm_execution
  if (!managed || typeof managed !== "object") return executionFor(sessionID)
  const value = managed as Record<string, unknown>
  if (!validID(value.execution_id)) throw new Error("larm_execution.execution_id must be a canonical UUIDv4")
  if (value.provider_id !== undefined && value.provider_id !== providerID) throw new Error("managed prompt provider does not match LARM")
  const current = executions.get(sessionID)
  if (current && current.executionID !== value.execution_id && activeFor(current.executionID)) {
    throw new Error("session already has an active LARM execution")
  }
  const ref: ExecutionRef = {
    executionID: value.execution_id,
    sessionID,
    modelID: typeof value.model_id === "string" ? value.model_id : undefined,
    parentExecutionID: typeof value.parent_execution_id === "string" ? value.parent_execution_id : undefined,
  }
  executions.set(sessionID, ref)
  emit("session_execution_bound", { execution_id: ref.executionID, session_id: sessionID, parent_execution_id: ref.parentExecutionID, model_id: ref.modelID, ownership: "DECLARED_UNVERIFIED" })
  return ref
}

function activeFor(executionID: string) {
  for (const ref of activeRequests.values()) if (ref.executionID === executionID && !ref.terminal) return true
  return false
}

function eventSessionID(event: any): string | undefined {
  const data = event?.data ?? event?.properties ?? {}
  if (typeof data.sessionID === "string") return data.sessionID
  if (typeof data.info?.id === "string") return data.info.id
  return undefined
}

function observeEvent(event: any) {
  const type = typeof event?.type === "string" ? event.type : "unknown"
  const sessionID = eventSessionID(event)
  if (!sessionID) {
    emit("opencode_event", { opencode_event: type })
    return
  }
  const ref = executions.get(sessionID) ?? executionFor(sessionID)
  emit("opencode_event", { opencode_event: type, execution_id: ref.executionID, session_id: sessionID, model_id: ref.modelID })
  if (type === "session.deleted") {
    ref.disposed = true
    if (!activeFor(ref.executionID)) executions.delete(sessionID)
  }
}

export default Plugin.define({
  id: "larm-opencode-adapter",
  async setup(ctx) {
    const observer = new AbortController()
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: observer.signal })) observeEvent(event)
      } catch (error) {
        if (!observer.signal.aborted) emit("observer_error", { error: error instanceof Error ? error.name : "unknown" })
      }
    })()

    await ctx.session.hook("prompt", async (event) => {
      bindPrompt(event.sessionID, event.metadata as Record<string, unknown> | undefined)
    })

    await ctx.session.hook("model.request", async (event) => {
      if (event.model.providerID !== providerID) return
      const identity = executionFor(event.sessionID, event.model.id)
      const requestID = randomUUID()
      const accept = String(event.headers.Accept ?? event.headers.accept ?? "text/event-stream")
      const stream = accept.includes("event-stream")
      event.headers["X-LARM-Execution-ID"] = identity.executionID
      event.headers["X-LARM-Request-ID"] = requestID
      emit("model_request", { execution_id: identity.executionID, session_id: event.sessionID, request_id: requestID, model_id: event.model.id, stream, ownership: "DECLARED_UNVERIFIED" })
    })

    await ctx.session.hook("http.request", async (event) => {
      const requestID = event.request.headers.get("x-larm-request-id")
      const executionID = event.request.headers.get("x-larm-execution-id")
      if (!requestID || !validID(requestID) || !executionID || !validID(executionID)) return
      const ref: RequestRef = { executionID, requestID, sessionID: event.sessionID, modelID: event.model.id, stream: (event.request.headers.get("accept") ?? "").includes("event-stream"), abortSignaled: event.request.signal.aborted }
      requests.set(event.request, ref)
      activeRequests.set(requestID, ref)
      event.request.signal.addEventListener("abort", () => {
        ref.abortSignaled = true
        emit("request_abort_signaled", { execution_id: ref.executionID, request_id: ref.requestID, session_id: ref.sessionID, model_id: ref.modelID, terminal_observed: Boolean(ref.terminal) })
      }, { once: true })
      emit("http_request", { execution_id: ref.executionID, request_id: ref.requestID, session_id: ref.sessionID, model_id: ref.modelID })
    })

    await ctx.session.hook("http.response", async (event) => {
      const ref = requests.get(event.request)
      if (!ref) return
      const stream = (event.response.headers.get("content-type") ?? "").includes("text/event-stream")
      let done = false
      let eof = false
      try {
        const reader = event.response.clone().body?.getReader()
        if (reader) {
          const decoder = new TextDecoder()
          let pending = ""
          while (true) {
            const part = await reader.read()
            if (part.done) { eof = true; break }
            if (!stream) continue
            pending += decoder.decode(part.value, { stream: true })
            const lines = pending.split(/\r?\n/)
            pending = lines.pop() ?? ""
            for (const line of lines) if (line.startsWith("data:") && line.slice(5).trim() === "[DONE]") done = true
          }
          if (pending.trim().replace(/^data:\s*/, "") === "[DONE]") done = true
        } else eof = true
        ref.terminal = stream ? (done && eof ? "SSE_COMPLETED" : "SSE_TRUNCATED") : "HTTP_COMPLETED"
        emit("http_observation", { execution_id: ref.executionID, request_id: ref.requestID, session_id: ref.sessionID, model_id: ref.modelID, http_status: event.response.status, sse_done: done, eof, diagnosis: ref.terminal })
      } catch (error) {
        ref.terminal = ref.abortSignaled ? "UNKNOWN" : "TRANSPORT_ERROR"
        emit("http_observation", { execution_id: ref.executionID, request_id: ref.requestID, session_id: ref.sessionID, model_id: ref.modelID, http_status: event.response.status, diagnosis: ref.terminal, error: error instanceof Error ? error.name : "unknown" })
      } finally {
        activeRequests.delete(ref.requestID)
        const identity = executions.get(ref.sessionID)
        if (identity?.disposed && !activeFor(identity.executionID)) executions.delete(ref.sessionID)
      }
    })

    return () => observer.abort()
  },
})
