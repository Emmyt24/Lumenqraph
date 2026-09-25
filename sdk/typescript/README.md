# @lumenqraph/sdk

Typed TypeScript client for the [Lumenqraph](https://github.com/Lumen-Scribe/Lumenqraph)
Soroban event-indexer API — REST and GraphQL, zero runtime dependencies (uses
the platform `fetch`; Node 18+ or the browser).

## Install

```bash
npm install @lumenqraph/sdk
```

## Quick start

```ts
import { LumenqraphClient } from "@lumenqraph/sdk";

const lq = new LumenqraphClient({
  baseUrl: "http://localhost:8080",
  apiKey: process.env.LUMENQRAPH_API_KEY, // only if REQUIRE_API_KEY is on
});

// Which contracts are indexed?
const contracts = await lq.listContracts();

// Recent decoded + typed events (REST, limit/offset).
const events = await lq.listEvents(contracts[0].contract_id, { limit: 20 });

// Current on-chain state (instance storage) and per-holder balances.
const state = await lq.getState(contracts[0].contract_id);
const balances = await lq.getData(contracts[0].contract_id, { label: "balance" });

// A read-only view call, type-checked against the on-chain spec.
const dec = await lq.call(contracts[0].contract_id, { function: "decimals" });
console.log(dec.result);

// Dry-run any call and preview its result, events, and cost.
const preview = await lq.simulate(contracts[0].contract_id, {
  function: "transfer",
  args: { from: "G...", to: "G...", amount: "100" },
});

// Retrieve a generated typed client for a contract.
const sdkCode = await lq.generateSdk(contracts[0].contract_id, { lang: "ts" });
```

## Retry, backoff, and timeout (#81)

The client retries transient failures automatically with **exponential backoff
+ full jitter** and honours the `Retry-After` header on `429` responses. Each
attempt gets its own timeout window via `AbortController`.

```ts
const lq = new LumenqraphClient({
  baseUrl: "http://localhost:8080",
  retry: {
    maxRetries:  3,       // default: 3  — set to 0 to disable
    baseDelayMs: 250,     // default: 250 ms
    maxDelayMs:  30_000,  // default: 30 s  cap on computed delay
    timeoutMs:   10_000,  // default: 10 s  per-request timeout
  },
});
```

Retried statuses: `429`, `502`, `503`, `504`, and any network/abort error.
All other non-2xx responses are thrown immediately as `LumenqraphError`.

## Webhook signature verification (#83)

Every webhook delivery carries an `X-Lumenqraph-Signature: sha256=<hmac>`
header. Use the `verifyWebhook` helper — it uses the Web Crypto API for a
**constant-time comparison** safe against timing attacks.

```ts
import { verifyWebhook } from "@lumenqraph/sdk";

// Express / Node example:
app.post("/hook", express.raw({ type: "*/*" }), async (req, res) => {
  const valid = await verifyWebhook(
    req.body,                                           // raw Buffer or string
    req.headers["x-lumenqraph-signature"] as string,   // "sha256=…"
    process.env.WEBHOOK_SECRET!,
  );
  if (!valid) return res.status(401).send("invalid signature");
  // process event …
  res.sendStatus(200);
});
```

`verifyWebhook(rawBody, signatureHeader, secret): Promise<boolean>`

- `rawBody` — `string` or `Uint8Array` (the raw, un-parsed request body).
- `signatureHeader` — the full `X-Lumenqraph-Signature` header value, e.g. `"sha256=abc123…"`.
- `secret` — the subscription secret returned at webhook creation.

## Cursor pagination

The GraphQL endpoint exposes Relay-style cursor connections. The SDK wraps them
as an async iterator that fetches page after page for you:

```ts
for await (const ev of lq.paginateEvents(contractId, { pageSize: 100 })) {
  console.log(ev.ledger, ev.event_name, ev.enriched ?? ev.decoded_value);
}
```

Or drive it a page at a time:

```ts
let page = await lq.eventsPage(contractId, { first: 50 });
while (page.hasNextPage) {
  page = await lq.eventsPage(contractId, { first: 50, after: page.endCursor! });
}
```

## Raw GraphQL

```ts
const data = await lq.graphql<{ transfers: { edges: { node: unknown }[] } }>(`
  query($id: String!) {
    transfers(contractId: $id, first: 10) {
      edges { node { fromAddr toAddr amount ledger } }
      pageInfo { hasNextPage endCursor }
    }
  }`,
  { id: contractId },
);
```

## API surface

| Method | Endpoint |
| --- | --- |
| `health()` | `GET /health` |
| `listContracts()` | `GET /contracts` |
| `getInterface(id)` | `GET /contracts/:id/interface` |
| `generateSdk(id, { lang, version })` | `GET /contracts/:id/sdk` |
| `getState(id, { limit })` | `GET /contracts/:id/state` |
| `getData(id, { label, limit })` | `GET /contracts/:id/data` |
| `getDataKey(id, keyHash, { limit })` | `GET /contracts/:id/data/:keyHash` |
| `listEvents(id, { limit, offset, eventName })` | `GET /contracts/:id/events` |
| `getStats(id, { resolution, window })` | `GET /contracts/:id/stats` |
| `listTransfers(id?, { limit, offset })` | `GET /contracts/:id/transfers` |
| `listFunctions(id)` | `GET /contracts/:id/functions` |
| `call(id, { function, args, sourceAccount })` | `POST /contracts/:id/call` |
| `simulate(id, { function, args, sourceAccount })` | `POST /contracts/:id/simulate` |
| `graphql(query, variables)` | `POST /graphql` |
| `eventsPage` / `paginateEvents` | `POST /graphql` (cursor) |
| `verifyWebhook(rawBody, sigHeader, secret)` *(standalone)* | — |

Errors for non-2xx responses are thrown as `LumenqraphError` (`.status`, `.body`).

## Generated types (#82)

SDK response types are generated from the canonical OpenAPI schema at
`openapi.yaml` (repo root) via `openapi-typescript`. The generated file lives
at `generated/api.d.ts` and is committed. CI fails if it drifts from the
schema. See [CODEGEN.md](CODEGEN.md) for the full workflow.

```bash
# Regenerate after editing openapi.yaml:
cd sdk/typescript
npm run codegen

# Verify (run in CI):
npm run codegen:check
```

## Build from source

```bash
cd sdk/typescript
npm install
npm run build        # emits dist/ (ESM + .d.ts)
npm test             # runs vitest (26 tests)
npm run typecheck    # tsc --noEmit
npm run codegen:check # verify generated types are up to date
```

## License

MIT
