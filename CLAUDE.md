# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm test                          # run all tests
npx jest tests/config.test.ts     # run a single test file
npx jest -t "reuses admin"        # run tests matching a name pattern
npm run typecheck                 # tsc --noEmit (no emit, type errors only)
npm run build                     # compile to dist/
npm run dev                       # run via tsx (no build step)
npm start                         # run compiled dist/index.js
```

**Running the server:**
```bash
# env var
KAFKA_BROKERS=localhost:9092 npm run dev

# CLI arg
npx tsx src/index.ts --broker localhost:9092,localhost:9093
```

## Architecture

The server is a stdio MCP server: it reads JSON-RPC from stdin and writes to stdout. Claude Desktop spawns it as a subprocess.

**Startup flow:** `config.ts` resolves broker addresses → `KafkaService` is instantiated → tools are registered on `McpServer` → `StdioServerTransport` connects.

**Layers:**

- `src/config.ts` — broker resolution only. `--broker` CLI arg takes priority over `KAFKA_BROKERS` env var; throws if neither is set.
- `src/services/kafka.service.ts` — all KafkaJS operations. Admin client is a lazy-connected singleton (promise-memoized to be safe under concurrent calls). Producers are short-lived per `produceMessage` call. Consumers are short-lived per `consumeMessages` call (`fromBeginning: true`, stops at `limit` messages or 5 s timeout).
- `src/tools/{topics,messages,consumers}.ts` — each file exports a `registerXxxTools(server, kafka)` function. Tools catch all errors and return `{ isError: true, content: [...] }` rather than throwing.
- `src/index.ts` — wires the above, registers SIGINT/SIGTERM shutdown.

**MCP SDK note:** `server.registerTool()` is used (not the deprecated `server.tool()`). Due to a type inference depth issue (TS2589) in SDK 1.x + Zod v3, all tool files cast `server` to a local `AnyMcpServer` type before calling `registerTool`. This is intentional — remove it when the SDK fixes the generic.

**`consumeMessages` behavior:** Creates a new consumer group per call. The default groupId is `"tmp"`. Each call re-reads from the beginning of the topic because `fromBeginning: true` is always set and offsets are not committed. Callers that pass a stable `groupId` will get offset-tracking across calls.

## Tests

Tests live in `tests/` mirroring `src/`. KafkaJS is fully mocked in service tests — no broker needed. Tool tests mock `KafkaService` with `jest.mock` and capture registered handlers via a fake `server` object.

The `consumeMessages` tests use `jest.useFakeTimers()` to control the 5 s timeout without real waiting.
