# LARM-006.5 two independent clients

The runner uses only Python's standard library. `client.py` is a separate
process for each client and communicates only with LARM's HTTP API. It never
opens the configured model port, starts `llama-server`, sends signals, or
writes prompts/responses to evidence.

Example with the local macOS installation:

```bash
python3 tests/integration/two_clients/runner.py \
  --start-larm \
  --model-a /Users/odin/AI/runtime/models/Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf \
  --model-b /Users/odin/AI/runtime/models/Qwen3-Coder-30B-A3B-Instruct-Q4_K_M.gguf
```

The runner creates an isolated temporary config, ports, journal,
idempotency store, process log, client metadata, snapshots and `summary.json`.
Use `--output-dir` to preserve the directory at a known path. The default
suite is non-destructive and runs the original real-GGUF PASS matrix.

LARM-006.5.1 scenarios are selected explicitly:

```bash
# controlled upstream faults through the real Process Supervisor
python3 tests/integration/two_clients/runner.py --start-larm \
  --scenario blocked --backend controlled

# real GGUF downstream disconnect, crash, and recovery plan
python3 tests/integration/two_clients/runner.py --start-larm \
  --scenario TEST-10 --backend real --allow-destructive \
  --model-a /path/model-a.gguf --model-b /path/model-b.gguf
```

`TEST-10`, `TEST-17` and `TEST-19` require `--allow-destructive`; without it
they are recorded as `BLOCKED`. No destructive action occurs on import.
