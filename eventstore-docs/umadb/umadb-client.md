# Rust Clients for UmaDB

The Rust crate [`umadb-client`](https://crates.io/crates/umadb-client) provides both **asynchronous** and **synchronous** clients for reading and
appending events in UmaDB via the UmaDB [gRPC API](/grpc-api).

The synchronous client functions effectively as a wrapper around the asynchronous client.

The Rust clients implement the same traits and types used internally in an UmaDB server, and so
effectively represent remotely the essential internal server operations, with gRPC used as a transport
layer for inter-process communication (IPC).

The clients and common DCB structs are described below, followed by some examples.

## Connection Builder

The `UmaDBClient::new` function is the main entry point for the Rust UmaDB clients.

```rust
pub fn new(url: String) -> Self
```

It must be called with a URL. It returns an instance of `UmaDBClient`. Optional configuration options
can be set with chainable methods. Asynchronous and synchronous client instances can then be constructed with
the accumulated configuration.

If the `url` has `"https"` or `"grpcs"` then a "secure" gRPC channel using TLS will be created.
Otherwise, an "insecure" channel without TLS will be used.

### Optional Configuration

When using TLS, if the server's root certificate is not installed locally, perhaps if the server is using
a self-signed certificate, then the path to a file containing the certificate can be provided by calling
`ca_path()`.

```rust
pub fn ca_path(self, ca_path: String) -> Self
```

The `ca_path` argument should be a path to the PEM-encoded root or CA certificate used to sign the server's certificate.

The client can be configured to use an API key for authentication by calling `api_key()`.

```rust
pub fn api_key(self, api_key: String) -> Self
```

The `api_key` argument should be a string containing the API key to use for authentication, and must match
the server's configured API key.

The `batch_size()` method can be used to set a hint for how many events to buffer per batch when reading events.

```rust
pub fn batch_size(self, batch_size: u32) -> Self
```

The `batch_size` value can modestly affect latency and throughput. The server will also cap this value at a reasonable level.
If unset, the server will use a sensible default batch size.

### Connecting to the Server

The `connect_async()` method returns an instance of [`AsyncUmaDbClient`](#asynchronous-client).

```rust
pub async fn connect_async(&self) -> DCBResult<AsyncUmaDBClient>
```

The `connect()` method returns an instance of [`SyncUmaDbClient`](#synchronous-client).

```rust
pub fn connect(&self) -> DCBResult<SyncUmaDBClient>
```


### Connection Examples

The examples below show how to construct synchronous and asynchronous connections to UmaDB.

::: tabs
== sync
```rust
use umadb_client::UmaDBClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Without TLS (insecure connection)
    let client = UmaDBClient::new("http://localhost:50051".to_string())
        .connect()?;

    // With batch size hint (insecure connection)
    let client = UmaDBClient::new("http://localhost:50051".to_string())
        .batch_size(1000)
        .connect()?;

    // With TLS (system CAs)
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .connect()?;

    // With TLS (system CAs) + API key
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect()?;

    // With TLS (self-signed)
    let client = UmaDBClient::new("https://localhost:50051".to_string())
        .ca_path("server.pem".to_string())
        .connect()?;

    // With TLS (self-signed) + API key
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .ca_path("server.pem".to_string())
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect()?;

    // With TLS (self-signed) + API key + batch size hint
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .ca_path("server.pem".to_string())
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .batch_size(1000)
        .connect()?;

    Ok(())
}
```
== async
```rust
use umadb_client::UmaDBClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Without TLS (insecure connection)
    let client = UmaDBClient::new("http://localhost:50051".to_string())
        .connect_async().await?;

    // With batch size hint (insecure connection)
    let client = UmaDBClient::new("http://localhost:50051".to_string())
        .batch_size(1000)
        .connect_async().await?;

    // With TLS (system CAs)
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .connect_async().await?;

    // With TLS (system CAs) + API key
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect_async().await?;

    // With TLS (self-signed)
    let client = UmaDBClient::new("https://localhost:50051".to_string())
        .ca_path("server.pem".to_string())
        .connect_async().await?;

    // With TLS (self-signed) + API key
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .ca_path("server.pem".to_string())
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect_async().await?;

    // With TLS (self-signed) + API key + batch size hint
    let client = UmaDBClient::new("https://example.com:50051".to_string())
        .ca_path("server.pem".to_string())
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .batch_size(1000)
        .connect_async().await?;

    Ok(())
}
```
:::

## Asynchronous Client

The asynchronous UmaDB client implements the client-side of the gRPC API, and provides an asynchronous Rust API
for interacting with an UmaDB event store.

## Synchronous Client

The synchronous client is a thin wrapper around the asynchronous client. It provides synchronous, but otherwise
identical methods, with blocking behavior.

## Appending Events

The `append()` method writes new events to an UmaDB server.

::: tabs
== sync
```rust
fn append(
    &self,
    events: Vec<DCBEvent>,
    condition: Option<DCBAppendCondition>,
    tracking_info: Option<TrackingInfo>,
) -> DCBResult<u64>
```
== async
```rust
async fn append(
    &self,
    events: Vec<DCBEvent>,
    condition: Option<DCBAppendCondition>,
    tracking_info: Option<TrackingInfo>,
) -> DCBResult<u64>
```
:::

The `append()` method can be used to append new [`DCBEvent`](#event) instances to UmaDB atomically,
with an optional append condition and optional tracking information. Events are written in order.

Conditional appends with event UUIDs are idempotent. The server does not enforce uniqueness of events IDs.

### Parameters

| Name            | Type                         | Description                                                                             |
|-----------------|------------------------------|-----------------------------------------------------------------------------------------|
| `events`        | `Vec<DCBEvent>`              | The list of events to append. Each includes an event type, tags, and data payload.      |
| `condition`     | `Option<DCBAppendCondition>` | Optional [append condition](#append-condition) to ensure no conflicting writes occur.   |
| `tracking_info` | `Option<TrackingInfo>`       | Optional [tracking information](#tracking-info) – for event-processing components only. |

### Return Value

Returns the sequence number (`u64`) of the last successfully appended event from this operation. This
value can be used to wait for downstream event-processing components in a CQRS system to become up-to-date.


## Reading Events

The `read()` method returns recorded events from an UmaDB server.

::: tabs
== sync
```rust
fn read(
    &self,
    query: Option<DCBQuery>,
    start: Option<u64>,
    backwards: bool,
    limit: Option<u32>,
    subscribe: bool,
) -> DCBResult<Box<dyn DCBReadResponseSync + Send + 'static>>
```
== async
```rust
async fn read(
    &self,
    query: Option<DCBQuery>,
    start: Option<u64>,
    backwards: bool,
    limit: Option<u32>,
    subscribe: bool,
) -> DCBResult<Box<dyn DCBReadResponseAsync + Send + 'static>>
```
:::

The `read()` method can be used both for constructing decision models in a domain layer, and for projecting events into
materialized views in CQRS. An optional [`DCBQuery`](#query) can be provided to select by tags and types.

### Parameters

| Name        | Type               | Description                                                                                                                                                  |
|-------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `query`     | `Option<DCBQuery>` | Optional structured query to filter events (by tags, event types, etc).                                                                                      |
| `start`     | `Option<u64>`      | Read events *from* this sequence number. Only events with positions greater than or equal will be returned (or less than or equal if `backwards` is `true`.  |
| `backwards` | `bool`             | If `true` events will be read backwards, either from the given position or from the last recorded event.                                                     |
| `limit`     | `Option<u32>`      | Optional cap on the number of events to retrieve.                                                                                                            |
| `subscribe` | `bool`             | If `true`, keeps the stream open to deliver future events as they arrive.                                                                                    |

### Return Value

Returns a "read response" instance from which [`DCBSequencedEvent`](#sequenced-event) instances, and the most relevant "last known" sequence number, can be obtained.


## Getting Head Position

The `head()` method returns the position of the last recorded event in an UmaDB server. 

::: tabs
== sync
```rust
fn head(self) -> DCBResult<Option<u64>>
```
== async
```rust
async fn head(self) -> DCBResult<Option<u64>>
```
:::

The `head()` method can be used for counting the number of recorded events in the database, or for determining the position
of the last recorded event when subscribing only to new events.

### Return Value

Returns the position (`u64`) of the last recorded event in the event store, or `None` if no events have been recorded yet.


## Getting Tracking Info

The `get_tracking_info()` method returns the last recorded position in an upstream sequence of events.

::: tabs
== sync
```rust
fn get_tracking_info(
    &self,
    source: String,
) -> DCBResult<Option<u64>>
```
== async
```rust
async fn get_tracking_info(
    &self,
    source: String,
) -> DCBResult<Option<u64>>
```
:::

The `get_tracking_info()` method can be used when starting or resuming an
event-processing component. The event-processing component will start by requesting new events from the upstream
sequence after this position. The position of an upstream event that has been processed successfully can be [recorded
atomically](#appending-events) when appending new events generated by processing that event.

### Parameters

| Name        | Type                         | Description             |
|-------------|------------------------------|-------------------------|
| `source`    | `String`                     | Upstream sequence name. |

### Return Value

Returns the last recorded upstream position (`u64`), or `None` if the sequence name is not found.

## Event

A `DCBEvent` represents a single event either to be appended or already stored in the event log.

| Field        | Type           | Description                                                   |
|--------------|----------------|---------------------------------------------------------------|
| `event_type` | `String`       | The event’s logical type (e.g. `"UserRegistered"`).           |
| `tags`       | `Vec<String>`  | Tags assigned to the event (used for filtering and indexing). |
| `data`       | `Vec<u8>`      | Binary payload associated with the event.                     |
| `uuid`       | `Option<Uuid>` | Unique event ID.                                              |

Idempotent support for append operations is activated by setting a UUID on appended events.

Include in:
* [Append requests](#appending-events) when writing new events to the store.

Included in:
* [`DCBSequencedEvent`](#sequenced-event) objects when the server responds to read requests.

Matched by:
* [`DCBQueryItem`](#query-item) during [`read()`](#reading-events) and [`append()`](#appending-events) operations.


## Append Condition

A `DCBAppendCondition` causes an append request to fail if events match its [`DCBQuery`](#query), optionally after
a sequence number.

| Field                  | Type          | Description                   |
|------------------------|---------------|-------------------------------|
| `fail_if_events_match` | `DCBQuery`    | Query for conflicting events. |
| `after`                | `Option<u64>` | Sequence number.              |

Include in:
* [Append requests](#appending-events) to define optimistic concurrent control.

To implement a consistency boundary, command handlers can use the same [`DCBQuery`](#query) used when
[reading events](#read-request) as the value of `fail_if_events_match`, and the "head" sequence
number received from the read response as the value of `after`.


## Tracking Info

A `TrackingInfo` instance indicates the source and position of an upstream event.

| Field      | Type     | Description               |
|------------|----------|---------------------------|
| `source`   | `String` | Upstream sequence name.   |
| `position` | `u64`    | Upstream sequence number. |

Include in:
* [Append requests](#appending-events) when recording the results of processing an upstream event.

To implement exactly-once semantics in event-processing components, pull events from an upstream
source after the [last recorded position](#getting-tracking-info), then record the upstream positions
of upstream events along with [new state](#appending-events) that results from processing those events.
By processing events sequentially in this way, each event will be processed at least once. And by
recording tracking information along with new state, the new state will be recorded at most once.
The combination of "at least once" processing and "at most once" recording gives "exactly once"
semantics from the point of view of consumers of the recorded state.


## Query

A `DCBQuery` defines criteria for selecting events in the event store.

| Field   | Type                | Description                                |
|---------|---------------------|--------------------------------------------|
| `items` | `Vec<DCBQueryItem>` | A list of selection criteria (logical OR). |

A [`DCBEvent`](#event) is selected if any [`DCBQueryItem`](#query-item) matches or the `items` field is empty.

Include in:
* [Read requests](#reading-events) to select events returned by the server.
* A [`DCBAppendCondition`](#append-condition) to select conflicting events.


## Query Item

A `DCBQueryItem` defines a criterion for matching events.

| Field   | Type          | Description                       |
|---------|---------------|-----------------------------------|
| `types` | `Vec<String>` | List of event types (logical OR). |
| `tags`  | `Vec<String>` | List of tags (logical AND).       |

A `DCBQueryItem` will match a [`DCBEvent`](#event) if:
* one of its `types` matches the [`DCBEvent.event_type`](#event) or its `types` field is empty; AND
* all of its `tags` match one of the [`DCBEvent.tags`](#event) or its `tags` field is empty.


## Sequenced Event

A `DCBSequencedEvent` represents a recorded [`DCBEvent`](#event) along with its assigned sequence number.

| Field      | Type       | Description          |
|------------|------------|----------------------|
| `position` | `u64`      | The sequence number. |
| `event`    | `DCBEvent` | The recorded event.  |

Included in:
* [Read responses](#reading-events) when the server responds to read requests.


## Error

The `DCBError` enum represents all errors that can occur in UmaDB.

| Variant                        | Description                                         |
|--------------------------------|-----------------------------------------------------|
| `TransportError(message)`      | Client-server connection failed.                    |
| `AuthenticationError(message)` | Client-server authentication failed.                |
| `IntegrityError(message)`      | Append condition failed or data integrity violated. |
| `InvalidArgument(message)`     | Invalid argument errors.                            |
| `Corruption(message)`          | Corruption detected in stored data.                 |
| `SerializationError(message)`  | Failure to serialize data to bytes.                 |
| `InternalError(message)`       | Unexpected internal server error.                   |
| `Io(error)`                    | Other errors.                                       |


## Result

`type DCBResult<T>` A convenience alias for results returned by the methods:

```rust
type DCBResult<T> = Result<T, DCBError>
```

All the client methods return this type, which yields either a successful result `T` or a `DCBError`.


## Examples

The examples below show how to use the Rust clients for UmaDB:

::: tabs
== sync
```rust
use umadb_client::UmaDBClient;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem, TrackingInfo,
};
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the gRPC server
    let url = "https://localhost:50051".to_string();
    let client = UmaDBClient::new(url)
        .ca_path("server.pem".to_string()) // For self-signed server certificates.
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect()?;

    // Define a consistency boundary
    let cb = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["example".to_string()],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        }],
    };

    // Read events for a decision model
    let mut read_response = client.read(Some(cb.clone()), None, false, None, false)?;

    // Build decision model
    while let Some(result) = read_response.next() {
        match result {
            Ok(event) => {
                println!(
                    "Got event at position {}: {:?}",
                    event.position, event.event
                );
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Remember the last-known position
    let last_known_position = read_response.head().unwrap();
    println!("Last known position is: {:?}", last_known_position);

    // Produce new event
    let event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
        uuid: Some(Uuid::new_v4()),
    };

    // Append event in consistency boundary
    let commit_position1 = client.append(
        vec![event.clone()],
        Some(DCBAppendCondition {
            fail_if_events_match: cb.clone(),
            after: last_known_position,
        }),
        None,
    )?;
    println!("Appended event at position: {}", commit_position1);

    // Append conflicting event - expect an error
    let conflicting_event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
        uuid: Some(Uuid::new_v4()), // different UUID
    };
    let conflicting_result = client.append(
        vec![conflicting_event],
        Some(DCBAppendCondition {
            fail_if_events_match: cb.clone(),
            after: last_known_position,
        }),
        None,
    );

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!("Conflicting event was rejected: {:?}", integrity_error);
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    // Conditional appends with event UUIDs are idempotent.
    println!(
        "Retrying to append event at position: {:?}",
        last_known_position
    );
    let commit_position2 = client.append(
        vec![event.clone()],
        Some(DCBAppendCondition {
            fail_if_events_match: cb.clone(),
            after: last_known_position,
        }),
        None,
    )?;

    if commit_position1 == commit_position2 {
        println!(
            "Append method returned same commit position: {}",
            commit_position2
        );
    } else {
        panic!("Expected idempotent retry!")
    }

    // Subscribe to all events for a projection
    let mut subscription = client.read(None, None, false, None, true)?;

    // Build an up-to-date view
    while let Some(result) = subscription.next() {
        match result {
            Ok(ev) => {
                println!("Processing event at {}: {:?}", ev.position, ev.event);
                if ev.position == commit_position2 {
                    println!("Projection has processed new event!");
                    break;
                }
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Track an upstream position
    let upstream_position = client.get_tracking_info("upstream")?;
    let next_upstream_position = upstream_position.unwrap_or(0) + 1;
    println!("Next upstream position: {next_upstream_position}");
    client.append(
        vec![],
        None,
        Some(TrackingInfo {
            source: "upstream".to_string(),
            position: next_upstream_position,
        }),
    )?;
    assert_eq!(
        next_upstream_position,
        client.get_tracking_info("upstream")?.unwrap()
    );
    println!("Upstream position tracked okay!");

    // Try recording the same upstream position
    let conflicting_result = client.append(
        vec![],
        None,
        Some(TrackingInfo {
            source: "upstream".to_string(),
            position: next_upstream_position,
        }),
    );

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!(
                "Conflicting upstream position was rejected: {:?}",
                integrity_error
            );
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    Ok(())
}
```
== async
```rust
use futures::StreamExt;
use umadb_client::UmaDBClient;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBQuery, DCBQueryItem,
    TrackingInfo,
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the gRPC server
    let url = "https://localhost:50051".to_string();
    let client = UmaDBClient::new(url)
        .ca_path("server.pem".to_string()) // For self-signed server certificates.
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect_async()
        .await?;

    // Define a consistency boundary
    let cb = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["example".to_string()],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        }],
    };

    // Read events for a decision model
    let mut read_response = client
        .read(Some(cb.clone()), None, false, None, false)
        .await?;

    // Build decision model
    while let Some(result) = read_response.next().await {
        match result {
            Ok(event) => {
                println!(
                    "Got event at position {}: {:?}",
                    event.position, event.event
                );
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Remember the last-known position
    let last_known_position = read_response.head().await?;
    println!("Last known position is: {:?}", last_known_position);

    // Produce new event
    let event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
        uuid: Some(Uuid::new_v4()),
    };

    // Append event in consistency boundary
    let commit_position1 = client
        .append(
            vec![event.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: cb.clone(),
                after: last_known_position,
            }),
            None,
        )
        .await?;
    println!("Appended event at position: {}", commit_position1);

    // Append conflicting event - expect an error
    let conflicting_event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
        uuid: Some(Uuid::new_v4()), // different UUID
    };
    let conflicting_result = client
        .append(
            vec![conflicting_event.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: cb.clone(),
                after: last_known_position,
            }),
            None,
        )
        .await;

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!("Conflicting event was rejected: {:?}", integrity_error);
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    // Conditional appends with event UUIDs are idempotent.
    println!(
        "Retrying to append event at position: {:?}",
        last_known_position
    );
    let commit_position2 = client
        .append(
            vec![event.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: cb.clone(),
                after: last_known_position,
            }),
            None,
        )
        .await?;

    if commit_position1 == commit_position2 {
        println!(
            "Append method returned same commit position: {}",
            commit_position2
        );
    } else {
        panic!("Expected idempotent retry!")
    }

    // Subscribe to all events for a projection
    let mut subscription = client.read(None, None, false, None, true).await?;

    // Build an up-to-date view
    while let Some(result) = subscription.next().await {
        match result {
            Ok(ev) => {
                println!("Processing event at {}: {:?}", ev.position, ev.event);
                if ev.position == commit_position2 {
                    println!("Projection has processed new event!");
                    break;
                }
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Track an upstream position
    let upstream_position = client.get_tracking_info("upstream").await?;
    let next_upstream_position = upstream_position.unwrap_or(0) + 1;
    println!("Next upstream position: {next_upstream_position}");
    client
        .append(
            vec![],
            None,
            Some(TrackingInfo {
                source: "upstream".to_string(),
                position: next_upstream_position,
            }),
        )
        .await?;
    assert_eq!(
        next_upstream_position,
        client.get_tracking_info("upstream").await?.unwrap()
    );
    println!("Upstream position tracked okay!");

    // Try recording the same upstream position
    let conflicting_result = client
        .append(
            vec![],
            None,
            Some(TrackingInfo {
                source: "upstream".to_string(),
                position: next_upstream_position,
            }),
        )
        .await;

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!(
                "Conflicting upstream position was rejected: {:?}",
                integrity_error
            );
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    Ok(())
}
```
:::
