# LinkLLM

## repo structure
```
linkllm/
├── libraries/
│   ├── python/
│   │   ├── link_llm/               → Core SDK
│   │   │   ├── protocol.py         → JSON schema & message spec
│   │   │   ├── client.py           → Client to connect to Bus / other agents
│   │   │   ├── bridge.py           → WebSocket/HTTP connector
│   │   │   ├── __init__.py
│   │   │
│   │   ├── server/                 → Context & Memory Bus (like a lightweight MCP server)
│   │   │   ├── bus.py              → Message routing between agents
│   │   │   ├── registry.py         → Memory persistence (Redis/SQLite)
│   │   │
│   │   ├── examples/               → Agent demos
│   │   │   ├── analyst_writer.py
│   │   │
│   │   ├── docs/                   → Python documentation
│   │   │   └── README.md
│   │   │
│   │   └── dashboard/              → For visualization (later)
│   │       └── api.py
│   │
│   └── typescript/                 → Later JS SDK
│
└── README.md

```

## architecture flow:

```
+------------------+
|   Agent Client   |   →  link_llm.client.Client
|------------------|
| - Handles async send/recv
| - Middleware, routing
| - Heartbeat, retry
+------------------+
          |
          v
+------------------+
|     Bridge       |   →  link_llm.bridge.*
|------------------|
| - WebSocket / HTTP / InMemory
| - Handles transport + reconnect
+------------------+
          |
          v
+------------------+
|   Message Bus    |   →  server.bus / registry
|------------------|
| - Routes messages between agents
| - Stores shared memory/context
+------------------+
          |
          v
+------------------+
|   LLM Agents     |
|------------------|
| - Writer, Analyzer, Critic etc.
+------------------+

```