# Lumenqraph Python SDK

A typed Python client over the Lumenqraph REST + GraphQL API, with zero external dependencies.

## Installation

```bash
pip install lumenqraph
```

## Quick Start

```python
from lumenqraph import LumenqraphClient

# Initialize the client
lq = LumenqraphClient(base_url="http://localhost:8080")

# Or with an API key
lq = LumenqraphClient(
    base_url="http://localhost:8080",
    api_key="your-api-key"
)

# Get contracts
contracts = lq.list_contracts()
print(f"Found {len(contracts['data'])} contracts")

# Get events for a contract (with pagination)
for event in lq.paginate_events(contracts['data'][0]['contract_id']):
    print(f"Event: {event['event_name']}")
    print(f"  Value: {event.get('enriched') or event.get('decoded_value')}")
```

## Features

### REST API Methods

- **Contracts**
  - `list_contracts(limit, offset)` - Get all contracts with pagination
  - `get_interface(contract_id, version)` - Get a contract's decoded interface
  - `get_state(contract_id, limit)` - Get contract state snapshots
  - `get_data(contract_id, label, limit)` - Get contract per-key data
  - `get_data_key(contract_id, key_hash, limit)` - Get a specific key's history

- **Events**
  - `list_events(contract_id, limit, offset, event_name, after)` - Get contract events
  - `paginate_events(contract_id, event_name)` - Iterate through all events (cursor pagination)

- **Transfers**
  - `list_transfers(contract_id, limit, offset)` - Get SEP-41 transfers

- **Read / Simulate**
  - `call(contract_id, function, args, source_account)` - Invoke a view function
  - `simulate(contract_id, function, args, source_account)` - Dry-run a call

- **Utilities**
  - `list_functions(contract_id)` - Get callable functions
  - `health()` - Check API health

### Pagination

The SDK supports cursor-based pagination for efficient paging through large result sets:

```python
# Cursor pagination (recommended for large datasets)
for event in lq.paginate_events(contract_id):
    print(event)

# Manual pagination with offset
response = lq.list_events(contract_id, limit=100, offset=0)
events = response['data']
has_more = response['has_more']
```

### Retry Policy

The client automatically retries requests with exponential backoff for transient failures:

```python
lq = LumenqraphClient(
    base_url="http://localhost:8080",
    retry={
        "max_retries": 3,           # Maximum number of retries (default 3)
        "base_delay_ms": 250,       # Initial delay in milliseconds (default 250)
        "max_delay_ms": 30_000,     # Maximum delay cap (default 30s)
        "timeout_ms": 10_000,       # Per-request timeout (default 10s)
    }
)
```

### Webhook Signature Verification

Verify incoming webhook signatures for security:

```python
from lumenqraph import verify_webhook_signature

def handle_webhook(request):
    signature = request.headers.get('X-Webhook-Signature')
    body = request.get_data(as_text=True)
    secret = "your-webhook-secret"

    if not verify_webhook_signature(body, signature, secret):
        return {"error": "Invalid signature"}, 401

    # Process the webhook
    data = json.loads(body)
    return {"ok": True}, 200
```

## Type Hints

The SDK is fully typed for IDE autocompletion and type checking:

```python
from lumenqraph import LumenqraphClient, EventRecord

lq = LumenqraphClient(base_url="http://localhost:8080")
events: dict = lq.list_events("CABC...")
```

## Error Handling

The SDK raises `LumenqraphError` for API errors:

```python
from lumenqraph import LumenqraphClient, LumenqraphError

lq = LumenqraphClient(base_url="http://localhost:8080")

try:
    result = lq.call("CABC...", function="get_balance")
except LumenqraphError as e:
    print(f"API error: {e}")
    print(f"Status: {e.status}")
    print(f"Response body: {e.body}")
```

## Examples

### List all events for a contract

```python
lq = LumenqraphClient(base_url="http://localhost:8080")

# Get all events using cursor pagination
count = 0
for event in lq.paginate_events("CABC..."):
    print(f"{event['event_name']}: {event['decoded_value']}")
    count += 1

print(f"Total events: {count}")
```

### Find a specific event type

```python
for event in lq.paginate_events("CABC...", event_name="transfer"):
    print(f"Transfer: {event['enriched']}")
```

### Get contract interface and call a function

```python
interface = lq.get_interface("CABC...")
print(f"Functions: {interface['interface']['functions']}")

# Call a view function
result = lq.call("CABC...", function="get_balance", args=["GXYZ..."])
print(f"Balance: {result['result']}")
```

### Simulate a transaction

```python
simulation = lq.simulate(
    "CABC...",
    function="transfer",
    args=["from", "to", "1000"]
)
print(f"Events: {simulation['events']}")
print(f"Cost: {simulation['min_resource_fee']} stroops")
```

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Building the Package

```bash
python -m build
```

### Publishing to PyPI

Releases are published automatically by the `pypi-publish` GitHub Actions workflow
when a `v*` tag is pushed. Pre-release tags (for example `v0.2.0-rc.1`) are published
to TestPyPI first; stable tags go to PyPI via Trusted Publishing (OIDC, no API token).
The workflow fails if the version in `pyproject.toml` does not match the tag.

To publish manually:

```bash
python -m build
python -m twine upload dist/*
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.
