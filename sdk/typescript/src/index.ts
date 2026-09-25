/**
 * Lumenqraph TypeScript SDK — a typed client over the Lumenqraph REST + GraphQL
 * API. Zero runtime dependencies: it uses the platform `fetch` (Node 18+ or the
 * browser).
 *
 * ```ts
 * import { LumenqraphClient } from "@lumenqraph/sdk";
 *
 * const lq = new LumenqraphClient({ baseUrl: "http://localhost:8080" });
 * const contracts = await lq.listContracts();
 * for await (const ev of lq.paginateEvents(contracts[0].contract_id)) {
 *   console.log(ev.event_name, ev.enriched ?? ev.decoded_value);
 * }
 * ```
 */

// ---- Types ----
//
// These types mirror the Lumenqraph REST + GraphQL API responses. They are kept
// in sync with the server by the `typecheck` CI step (which catches obvious
// structural drift) and, once #44 (OpenAPI) lands, will be fully generated from
// `/openapi.json` via openapi-typescript. See sdk/typescript/CODEGEN.md for the
// planned workflow.

export type Json = unknown;

export interface Contract {
  contract_id: string;
  event_count: number;
  first_seen_ledger: number | null;
  last_seen_ledger: number | null;
}

export interface EventRecord {
  event_id: string;
  contract_id: string;
  ledger: number;
  ledger_closed_at: string;
  event_type: string;
  topics: string[];
  decoded_topics: Json;
  event_name: string | null;
  value: string;
  decoded_value: Json;
  /** Named, typed record from the contract's on-chain spec; null when none. */
  enriched: Json | null;
  tx_hash: string;
  in_successful_call: boolean;
  paging_token: string;
  created_at: string;
}

export interface Transfer {
  event_id: string;
  contract_id: string;
  from_addr: string | null;
  to_addr: string | null;
  amount: string;
  ledger: number;
  ledger_closed_at: string;
}

export interface StateVersion {
  ledger: number;
  storage: Json;
  captured_at: string;
}

export interface ContractState {
  contract_id: string;
  count: number;
  versions: StateVersion[];
}

export interface DataKey {
  key_hash: string;
  key: Json;
  durability: string;
  ledger: number;
  value: Json;
  label: string | null;
  captured_at: string;
}

export interface ContractData {
  contract_id: string;
  count: number;
  keys: DataKey[];
}

export interface DataKeyHistory {
  contract_id: string;
  key_hash: string;
  key: Json;
  durability: string;
  label: string | null;
  count: number;
  versions: { ledger: number; value: Json; captured_at: string }[];
}

export interface ContractsResponse {
  data: Contract[];
  has_more: boolean;
  /** Pass as `after` on the next call to fetch the next page. `null` when no more pages. */
  next_cursor: string | null;
}

export interface EventsResponse {
  data: EventRecord[];
  has_more: boolean;
  next_cursor: string | null;
}

export interface StatsBucket {
  bucket: string;
  count: number;
  breakdown?: Record<string, number>;
}

export interface StatsResponse {
  data: StatsBucket[];
  total: number;
}

export interface GetStatsOptions extends RequestOptions {
  resolution?: "1m" | "1h" | "1d" | "1w" | "1M" | "hour" | "day" | string;
  window?: string;
  bucket?: "hour" | "day" | "ledger" | string;
  groupBy?: "event_name" | string;
  from?: string;
  to?: string;
  fromLedger?: number;
  toLedger?: number;
}

export interface CallResult {
  contract_id: string;
  function: string;
  result: Json;
  simulated_at_ledger: number;
  /** Present for `simulate`: the events the call would emit. */
  events?: Json[];
  /** Present for `simulate`: the minimum resource fee, in stroops. */
  min_resource_fee?: string;
}

export interface CallOptions {
  function: string;
  /** Arguments: an object keyed by parameter name, or a positional array. */
  args?: Json;
  /** Optional `G…` source account for the simulation. */
  sourceAccount?: string;
}

export interface Webhook {
  id: string;
  url: string;
  subscriptions: string[];
  active: boolean;
  secret: string;
  created_at: string;
  updated_at: string;
}

export interface WebhookDelivery {
  id: string;
  webhook_id: string;
  status: "pending" | "success" | "failed";
  status_code?: number;
  error?: string;
  attempts: number;
  created_at: string;
  updated_at: string;
}

export interface CreateWebhookOptions {
  url: string;
  subscriptions?: string[];
}

export interface UpdateWebhookOptions {
  url?: string;
  subscriptions?: string[];
  active?: boolean;
}

/** A Relay-style page returned by the GraphQL cursor connections. */
export interface Page<T> {
  nodes: T[];
  endCursor: string | null;
  hasNextPage: boolean;
}

// ---- Retry / timeout options (#81) ----

/**
 * Retry policy applied to every request made by the client.
 * All fields are optional; the client merges them with sensible defaults.
 */
export interface RetryOptions {
  /**
   * Maximum number of retry attempts after the first failure.
   * Default: 3.
   */
  maxRetries?: number;
  /**
   * Base delay in milliseconds for the first retry.
   * Subsequent delays grow exponentially (base * 2^attempt).
   * Default: 250 ms.
   */
  baseDelayMs?: number;
  /**
   * Hard cap on the computed delay before jitter, in milliseconds.
   * Default: 30 000 ms (30 s).
   */
  maxDelayMs?: number;
  /**
   * Per-request wall-clock timeout in milliseconds. The SDK cancels the
   * underlying `fetch` after this many milliseconds via `AbortController`.
   * Each retry gets a fresh timeout window.
   * Default: 10 000 ms (10 s).
   */
  timeoutMs?: number;
}

export interface ClientOptions {
  /** Base URL of the Lumenqraph API, e.g. `http://localhost:8080`. */
  baseUrl: string;
  /** API key, sent as `x-api-key` when `REQUIRE_API_KEY` is enabled. */
  apiKey?: string;
  /** Override the fetch implementation (defaults to global `fetch`). */
  fetch?: typeof fetch;
  /**
   * Retry / timeout policy.
   * Retries are attempted for network errors and HTTP 429 / 502 / 503 / 504.
   * Pass `{ maxRetries: 0 }` to disable retries entirely.
   */
  retry?: RetryOptions;
}

export interface RequestOptions {
  /** Optional AbortSignal to cancel the request. */
  signal?: AbortSignal;
}

/** Error thrown for any non-2xx API response. */
export class LumenqraphError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly body: unknown,
  ) {
    super(message);
    this.name = "LumenqraphError";
  }
}

// ---- Internal constants ----

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BASE_DELAY_MS = 250;
const DEFAULT_MAX_DELAY_MS = 30_000;
const DEFAULT_TIMEOUT_MS = 10_000;

/** HTTP status codes that merit a retry. */
const RETRYABLE_STATUSES = new Set([429, 502, 503, 504]);

// ---- Client ----

export class LumenqraphClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly doFetch: typeof fetch;
  private readonly retry: Required<RetryOptions>;

  constructor(opts: ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.apiKey = opts.apiKey;
    const f = opts.fetch ?? globalThis.fetch;
    if (!f) {
      throw new Error(
        "no fetch implementation available; pass one via ClientOptions.fetch",
      );
    }
    this.doFetch = f.bind(globalThis);
    this.retry = {
      maxRetries:   opts.retry?.maxRetries   ?? DEFAULT_MAX_RETRIES,
      baseDelayMs:  opts.retry?.baseDelayMs  ?? DEFAULT_BASE_DELAY_MS,
      maxDelayMs:   opts.retry?.maxDelayMs   ?? DEFAULT_MAX_DELAY_MS,
      timeoutMs:    opts.retry?.timeoutMs    ?? DEFAULT_TIMEOUT_MS,
    };
  }

  // ---- REST ----

  /** Liveness + indexing-lag report. */
  health(opts: RequestOptions = {}): Promise<Json> {
    return this.get("/health", {}, opts.signal);
  }

  /**
   * Contracts the indexer has seen, with per-contract event counts.
   * Supports cursor-based pagination: pass the `next_cursor` from a previous
   * response as `after` to fetch the next page.
   */
  listContracts(
    opts: { limit?: number; after?: string; signal?: AbortSignal } = {},
  ): Promise<ContractsResponse> {
    return this.get("/contracts", { limit: opts.limit, after: opts.after }, opts.signal);
  }

  /** A contract's decoded on-chain interface (functions, events, types). */
  getInterface(contractId: string, opts: RequestOptions = {}): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/interface`, {}, opts.signal);
  }

  /** The version history of a contract's interface with semantic diffs. */
  getInterfaceHistory(
    contractId: string,
    opts: { limit?: number; signal?: AbortSignal } = {},
  ): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/interface/history`, {
      limit: opts.limit,
    }, opts.signal);
  }

  /** The semantic diff between any two interface versions of a contract. */
  getInterfaceDiff(
    contractId: string,
    opts: { from?: number; to?: number; signal?: AbortSignal } = {},
  ): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/interface/diff`, {
      from: opts.from,
      to: opts.to,
    }, opts.signal);
  }

  /** A historical version of a contract's interface. */
  getInterfaceAtVersion(
    contractId: string,
    version: number,
    opts: RequestOptions = {},
  ): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/interface`, {
      version,
    }, opts.signal);
  }

  /**
   * Retrieve a generated SDK client for a contract.
   * Returns the generated source code as a string.
   */
  generateSdk(
    contractId: string,
    opts?: { lang?: "ts"; version?: number } & RequestOptions,
  ): Promise<string> {
    return this.get<string>(
      `/contracts/${enc(contractId)}/sdk`,
      {
        lang: opts?.lang,
        version: opts?.version,
      },
      opts?.signal,
    );
  }

  /**
   * Retrieve time-bucketed event statistics for a contract.
   */
  getStats(
    contractId: string,
    opts: GetStatsOptions = {},
  ): Promise<StatsResponse> {
    return this.get<StatsResponse>(
      `/contracts/${enc(contractId)}/stats`,
      {
        resolution: opts.resolution,
        window: opts.window,
        bucket: opts.bucket,
        group_by: opts.groupBy,
        from: opts.from,
        to: opts.to,
        from_ledger: opts.fromLedger,
        to_ledger: opts.toLedger,
      },
      opts.signal,
    );
  }

  /** Versioned instance-storage snapshots, newest first (`limit=1` = current). */
  getState(contractId: string, opts: { limit?: number; signal?: AbortSignal } = {}): Promise<ContractState> {
    return this.get(`/contracts/${enc(contractId)}/state`, { limit: opts.limit }, opts.signal);
  }

  /** Latest value of every per-key entry (e.g. holder balances). */
  getData(
    contractId: string,
    opts: { label?: string; limit?: number; signal?: AbortSignal } = {},
  ): Promise<ContractData> {
    return this.get(`/contracts/${enc(contractId)}/data`, {
      label: opts.label,
      limit: opts.limit,
    }, opts.signal);
  }

  /** The version history of a single per-key entry (e.g. one balance). */
  getDataKey(
    contractId: string,
    keyHash: string,
    opts: { limit?: number; signal?: AbortSignal } = {},
  ): Promise<DataKeyHistory> {
    return this.get(`/contracts/${enc(contractId)}/data/${enc(keyHash)}`, {
      limit: opts.limit,
    }, opts.signal);
  }

  /** Recent events for a contract, newest first (limit/offset). */
  listEvents(
    contractId: string,
    opts: { limit?: number; offset?: number; eventName?: string; signal?: AbortSignal } = {},
  ): Promise<EventsResponse> {
    return this.get(`/contracts/${enc(contractId)}/events`, {
      limit: opts.limit,
      offset: opts.offset,
      event_name: opts.eventName,
    }, opts.signal);
  }

  /** Fetch a single event by its unique ID. */
  getEvent(eventId: string, opts: RequestOptions = {}): Promise<EventRecord> {
    return this.get(`/events/${enc(eventId)}`, {}, opts.signal);
  }

  /** All indexed events emitted by a transaction, in emission order. */
  getTransactionEvents(
    txHash: string,
    opts: { limit?: number; signal?: AbortSignal } = {},
  ): Promise<{ tx_hash: string; count: number; data: EventRecord[] }> {
    return this.get(`/transactions/${enc(txHash)}/events`, {
      limit: opts.limit,
    }, opts.signal);
  }

  /** Materialized SEP-41 transfers, newest first (limit/offset). */
  listTransfers(
    contractId?: string,
    opts: { limit?: number; offset?: number; signal?: AbortSignal } = {},
  ): Promise<Transfer[]> {
    const path = contractId
      ? `/contracts/${enc(contractId)}/transfers`
      : `/transfers`;
    return this.get(path, { limit: opts.limit, offset: opts.offset }, opts.signal);
  }

  /** A contract's callable view functions and their typed signatures. */
  listFunctions(contractId: string, opts: RequestOptions = {}): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/functions`, {}, opts.signal);
  }

  /** Invoke a view function read-only and get a typed result. */
  call(contractId: string, opts: CallOptions & RequestOptions): Promise<CallResult> {
    return this.post(`/contracts/${enc(contractId)}/call`, {
      function: opts.function,
      args: opts.args ?? null,
      source_account: opts.sourceAccount,
    }, opts.signal);
  }

  /** Dry-run any call and preview its result, emitted events, and cost. */
  simulate(contractId: string, opts: CallOptions & RequestOptions): Promise<CallResult> {
    return this.post(`/contracts/${enc(contractId)}/simulate`, {
      function: opts.function,
      args: opts.args ?? null,
      source_account: opts.sourceAccount,
    }, opts.signal);
  }

  // ---- Webhooks ----

  /** Create a new webhook subscription. Returns the webhook with its secret. */
  createWebhook(opts: CreateWebhookOptions & RequestOptions): Promise<Webhook> {
    return this.post("/webhooks", {
      url: opts.url,
      subscriptions: opts.subscriptions ?? [],
    }, opts.signal);
  }

  /** List all webhooks for this instance. */
  listWebhooks(opts: RequestOptions = {}): Promise<Webhook[]> {
    return this.get("/webhooks", {}, opts.signal);
  }

  /** Delete a webhook by ID. */
  deleteWebhook(id: string, opts: RequestOptions = {}): Promise<void> {
    return this.delete(`/webhooks/${enc(id)}`, opts.signal);
  }

  /** Update a webhook's URL, subscriptions, or active status. */
  updateWebhook(id: string, opts: UpdateWebhookOptions & RequestOptions): Promise<Webhook> {
    return this.post(`/webhooks/${enc(id)}`, {
      url: opts.url,
      subscriptions: opts.subscriptions,
      active: opts.active,
    }, opts.signal);
  }

  /** List delivery attempts for a webhook. */
  listDeliveries(id: string, opts: { limit?: number; offset?: number; signal?: AbortSignal } = {}): Promise<WebhookDelivery[]> {
    return this.get(`/webhooks/${enc(id)}/deliveries`, {
      limit: opts.limit,
      offset: opts.offset,
    }, opts.signal);
  }

  // ---- GraphQL ----

  /** Execute a raw GraphQL query against `/graphql`. */
  async graphql<T = Json>(
    query: string,
    variables: Record<string, unknown> = {},
    opts: RequestOptions = {},
  ): Promise<T> {
    const body = await this.post<{ data?: T; errors?: { message: string }[] }>(
      "/graphql",
      { query, variables },
      opts.signal,
    );
    if (body.errors?.length) {
      throw new LumenqraphError(
        `GraphQL error: ${body.errors.map((e) => e.message).join("; ")}`,
        200,
        body.errors,
      );
    }
    return body.data as T;
  }

  /** One cursor page of events via GraphQL. */
  async eventsPage(
    contractId: string,
    opts: { first?: number; after?: string; eventName?: string; signal?: AbortSignal } = {},
  ): Promise<Page<EventRecord>> {
    const query = `
      query Events($id: String!, $name: String, $first: Int, $after: String) {
        events(contractId: $id, eventName: $name, first: $first, after: $after) {
          edges { cursor node {
            eventId contractId ledger ledgerClosedAt eventType eventName
            decodedTopics decodedValue enriched txHash inSuccessfulCall
          } }
          pageInfo { hasNextPage endCursor }
        }
      }`;
    const data = await this.graphql<{
      events: {
        edges: { cursor: string; node: Record<string, unknown> }[];
        pageInfo: { hasNextPage: boolean; endCursor: string | null };
      };
    }>(query, {
      id: contractId,
      name: opts.eventName ?? null,
      first: opts.first ?? 50,
      after: opts.after ?? null,
    }, { signal: opts.signal });
    return {
      nodes: data.events.edges.map((e) => e.node as unknown as EventRecord),
      endCursor: data.events.pageInfo.endCursor,
      hasNextPage: data.events.pageInfo.hasNextPage,
    };
  }

  /**
   * Async iterator over *all* of a contract's events via GraphQL cursor
   * pagination — transparently fetching page after page.
   *
   * Cancellation (#279): pass `signal` to cancel from the caller. Independently,
   * breaking out of the `for await` loop early runs this generator's `finally`
   * cleanup, which aborts the signal handed to the page fetches — so an
   * in-flight `eventsPage` request is cancelled rather than left to run and have
   * its result discarded.
   */
  async *paginateEvents(
    contractId: string,
    opts: { pageSize?: number; eventName?: string; signal?: AbortSignal } = {},
  ): AsyncGenerator<EventRecord> {
    // An internal controller, chained to the caller's signal, is what the page
    // fetches actually see. Early termination (a `break` in the consumer's
    // `for await`, or a thrown error) triggers the `finally` below, which aborts
    // it and tears down any pending request.
    const pageAborter = new AbortController();
    const external = opts.signal;
    const onExternalAbort = () => pageAborter.abort(external?.reason);
    if (external) {
      if (external.aborted) pageAborter.abort(external.reason);
      else external.addEventListener("abort", onExternalAbort, { once: true });
    }

    try {
      let after: string | undefined;
      for (;;) {
        const page = await this.eventsPage(contractId, {
          first: opts.pageSize ?? 100,
          after,
          eventName: opts.eventName,
          signal: pageAborter.signal,
        });
        for (const node of page.nodes) yield node;
        if (!page.hasNextPage || !page.endCursor) return;
        after = page.endCursor;
      }
    } finally {
      external?.removeEventListener("abort", onExternalAbort);
      pageAborter.abort();
    }
  }

  /**
   * Async iterator over *all* of a contract's events via REST cursor pagination.
   * Supports richer filters (topic, param, ledger range, time range) compared to GraphQL.
   * Transparently fetches page after page until all events are consumed.
   */
  async *paginateEventsRest(
    contractId: string,
    opts: {
      limit?: number;
      eventName?: string;
      fromLedger?: number;
      toLedger?: number;
      since?: string;
      until?: string;
      topic0?: string;
      topic1?: string;
      topic2?: string;
      topic3?: string;
      param?: string;
      signal?: AbortSignal;
    } = {},
  ): AsyncGenerator<EventRecord> {
    let nextCursor: string | undefined;
    for (;;) {
      const response = await this.get<EventsResponse>(
        `/contracts/${enc(contractId)}/events`,
        {
          limit: opts.limit ?? 100,
          event_name: opts.eventName,
          from_ledger: opts.fromLedger,
          to_ledger: opts.toLedger,
          since: opts.since,
          until: opts.until,
          topic0: opts.topic0,
          topic1: opts.topic1,
          topic2: opts.topic2,
          topic3: opts.topic3,
          param: opts.param,
          after: nextCursor,
        },
        opts.signal,
      );
      for (const event of response.data) yield event;
      if (!response.next_cursor || !response.has_more) return;
      nextCursor = response.next_cursor;
    }
  }

  // ---- Internals ----

  private async get<T = Json>(
    path: string,
    query: Record<string, unknown> = {},
    signal?: AbortSignal,
  ): Promise<T> {
    const url = new URL(this.baseUrl + path);
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    }
    return this.request<T>(url.toString(), { method: "GET" }, signal);
  }

  private post<T = Json>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
    return this.request<T>(this.baseUrl + path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }, signal);
  }

  private async delete<T = Json>(path: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>(this.baseUrl + path, {
      method: "DELETE",
    }, signal);
  }

  /**
   * Core fetch wrapper with retry + timeout (#81, #142).
   *
   * Retry policy:
   *  - Network errors (fetch throws) are always retried.
   *  - HTTP 429: honors `Retry-After` (seconds or HTTP-date) before retrying.
   *  - HTTP 502 / 503 / 504: retried with exponential backoff + full jitter.
   *  - Any other non-2xx: thrown immediately as `LumenqraphError`.
   *
   * Each attempt gets its own `AbortController` so the timeout window resets
   * after every retry — a slow response on attempt 1 doesn't eat the budget
   * for attempt 2. An external AbortSignal is checked before attempting retries,
   * allowing cancellation from the caller.
   */
  private async request<T>(url: string, init: RequestInit, signal?: AbortSignal): Promise<T> {
    const { maxRetries, baseDelayMs, maxDelayMs, timeoutMs } = this.retry;
    let attempt = 0;

    for (;;) {
      // Check if externally aborted before making the request.
      if (signal?.aborted) {
        const err = new Error("AbortError");
        err.name = "AbortError";
        throw err;
      }

      // Fresh AbortController each attempt so the timeout window resets.
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);

      // Listen for external abort signal and propagate it.
      const abortListener = () => controller.abort();
      signal?.addEventListener("abort", abortListener);

      let res: Response;
      let text: string;
      try {
        const headers = new Headers(init.headers);
        if (this.apiKey) headers.set("x-api-key", this.apiKey);
        res = await this.doFetch(url, {
          ...init,
          headers,
          signal: controller.signal,
        });
        text = await res.text();
      } catch (err) {
        // Network error or timeout (AbortError).
        if (attempt < maxRetries) {
          await sleep(jitteredDelay(attempt, baseDelayMs, maxDelayMs));
          attempt++;
          continue;
        }
        throw err;
      } finally {
        clearTimeout(timer);
        signal?.removeEventListener("abort", abortListener);
      }

      const parsed = text ? safeJson(text) : null;

      if (!res.ok) {
        // Retryable status?
        if (RETRYABLE_STATUSES.has(res.status) && attempt < maxRetries) {
          const wait = retryAfterMs(res) ?? jitteredDelay(attempt, baseDelayMs, maxDelayMs);
          await sleep(wait);
          attempt++;
          continue;
        }
        const message =
          (parsed as { error?: string } | null)?.error ??
          `${res.status} ${res.statusText}`;
        throw new LumenqraphError(message, res.status, parsed ?? text);
      }

      return parsed as T;
    }
  }
}

// ---- Webhook signature verification (#83) ----

/**
 * Verify a Lumenqraph webhook delivery using its HMAC-SHA256 signature.
 *
 * The server signs the raw request body with the subscription secret and sends
 * the result as `X-Lumenqraph-Signature: sha256=<hex>`. Pass that header value
 * as `signatureHeader` and the **raw** (un-parsed) request body as either a
 * `string` or `Uint8Array`.
 *
 * Comparison is performed in constant time via the Web Crypto API so this
 * helper is safe to use in security-sensitive contexts. It mirrors the
 * server-side `verify_hmac_signature()` in `lumenqraph-core/src/crypto.rs`.
 *
 * @param rawBody        Raw HTTP request body (string or bytes).
 * @param signatureHeader Value of the `X-Lumenqraph-Signature` header,
 *                        e.g. `"sha256=abcdef…"`.
 * @param secret         The subscription secret returned at creation time.
 * @returns              `true` if the signature is valid, `false` otherwise.
 *
 * @example
 * // Express.js / Node
 * import express from "express";
 * import { verifyWebhook } from "@lumenqraph/sdk";
 *
 * app.post("/hook", express.raw({ type: "*\/*" }), async (req, res) => {
 *   const valid = await verifyWebhook(
 *     req.body,
 *     req.headers["x-lumenqraph-signature"] as string,
 *     process.env.WEBHOOK_SECRET!,
 *   );
 *   if (!valid) return res.status(401).send("invalid signature");
 *   // process req.body ...
 *   res.sendStatus(200);
 * });
 */
export async function verifyWebhook(
  rawBody: string | Uint8Array,
  signatureHeader: string,
  secret: string,
): Promise<boolean> {
  // Parse off the "sha256=" prefix. An absent or wrong prefix is an invalid
  // signature, not a fatal error.
  const prefix = "sha256=";
  if (!signatureHeader.startsWith(prefix)) return false;
  const providedHex = signatureHeader.slice(prefix.length);

  // Encode inputs.
  const enc = new TextEncoder();
  // `.buffer as ArrayBuffer` cast: TextEncoder returns Uint8Array<ArrayBufferLike>
  // but Web Crypto expects ArrayBuffer specifically.  The underlying buffer is
  // always a plain ArrayBuffer here; the cast is safe.
  const keyBuffer = enc.encode(secret).buffer as ArrayBuffer;
  const bodyBytes: ArrayBuffer =
    typeof rawBody === "string"
      ? (enc.encode(rawBody).buffer as ArrayBuffer)
      : (rawBody.buffer.slice(rawBody.byteOffset, rawBody.byteOffset + rawBody.byteLength) as ArrayBuffer);

  // Import the secret as an HMAC-SHA-256 key via Web Crypto (Node 18+, browsers).
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    keyBuffer,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );

  // Compute the expected signature.
  const sigBuffer = await crypto.subtle.sign("HMAC", cryptoKey, bodyBytes);
  const expectedHex = bufToHex(sigBuffer);

  // Constant-time comparison: convert both hex strings to bytes and use
  // timingSafeEqual-equivalent logic. We compare byte arrays of the same
  // length so a length mismatch (different-length hex) also returns false
  // without short-circuiting.
  if (expectedHex.length !== providedHex.length) return false;

  const expectedBytes = enc.encode(expectedHex);
  const providedBytes = enc.encode(providedHex);

  // XOR every byte and accumulate — only equal if all XORs are 0.
  let diff = 0;
  for (let i = 0; i < expectedBytes.length; i++) {
    // biome-ignore lint: intentional constant-time compare
    diff |= (expectedBytes[i] ?? 0) ^ (providedBytes[i] ?? 0);
  }
  return diff === 0;
}

// ---- Retry helpers (#81) ----

/**
 * Exponential backoff with full jitter.
 *
 * Computes `random(0, min(maxDelayMs, baseDelayMs * 2^attempt))`.
 * Full jitter (vs. capped jitter) avoids thundering-herd when many clients
 * retry at the same time after a 503.
 */
function jitteredDelay(attempt: number, baseMs: number, maxMs: number): number {
  const cap = Math.min(maxMs, baseMs * Math.pow(2, attempt));
  return Math.random() * cap;
}

/**
 * Parse a `Retry-After` response header into milliseconds.
 *
 * The header may be:
 *  - A non-negative integer: number of seconds to wait.
 *  - An HTTP-date: an absolute point in time.
 *
 * Returns `undefined` when the header is absent or unparseable so the caller
 * can fall back to its own backoff strategy.
 */
function retryAfterMs(res: Response): number | undefined {
  const header = res.headers.get("retry-after");
  if (!header) return undefined;

  // Try integer seconds first.
  const seconds = Number(header.trim());
  if (!isNaN(seconds) && seconds >= 0) return seconds * 1000;

  // Try an HTTP-date.
  const date = new Date(header).getTime();
  if (!isNaN(date)) {
    const delta = date - Date.now();
    return delta > 0 ? delta : 0;
  }

  return undefined;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---- Helpers ----

function enc(segment: string): string {
  return encodeURIComponent(segment);
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function bufToHex(buf: ArrayBuffer): string {
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}
