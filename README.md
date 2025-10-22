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