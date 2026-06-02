# Chapter 5: Apache Arrow Flight SQL

## Two Entry Points

Sail exposes two gRPC services on two different ports (by default):

- **Spark Connect** (`:50051`) — the primary interface, designed for PySpark clients
- **Arrow Flight SQL** (`:50052`) — a secondary interface for any ADBC-compatible client, JDBC driver, or tool that speaks Arrow Flight natively

Spark Connect is described in Chapter 2. This chapter covers Flight SQL.

## What Is Arrow Flight?

Arrow Flight is a gRPC-based protocol designed specifically for bulk data transfer of Arrow data. Regular gRPC uses Protobuf for both control messages and data; Flight uses Protobuf for control messages but Arrow IPC for the data payload. This means there is no per-row serialization overhead for result data — the columnar bytes flow from server to client with minimal transformation.

Arrow Flight defines two foundational operations:
- **`DoGet(Ticket) → stream<FlightData>`**: given a ticket, stream Arrow data back
- **`DoPut(stream<FlightData>) → stream<PutResult>`**: stream Arrow data to the server

Arrow Flight SQL layers a SQL query interface on top of Flight. The key addition is a two-phase protocol:

1. **`GetFlightInfo(CommandStatementQuery)`** → `FlightInfo`: parse and plan the query, return a `FlightInfo` containing a `Ticket` (an opaque handle to the planned query)
2. **`DoGet(Ticket)`** → stream of `FlightData`: execute the planned query and stream results

This two-phase design allows clients to inspect the schema *before* fetching data (the `FlightInfo` includes the output schema), and it allows the server to pipeline execution: planning and execution can overlap with data transfer.

## `SailFlightSqlService`

Sail implements Arrow Flight SQL in `crates/sail-flight/src/service.rs`:

```rust
pub struct SailFlightSqlService {
    session_manager: SessionManager,
    config: Arc<PlanConfig>,
    metrics: Option<Arc<MetricRegistry>>,
    state: Arc<Mutex<SailFlightSqlState>>,
}

#[tonic::async_trait]
impl FlightSqlService for SailFlightSqlService {
    type FlightService = SailFlightSqlService;
    // ...
}
```

`FlightSqlService` is the trait from the `arrow-flight` crate. Sail only needs to implement the methods it supports; the rest default to `UNIMPLEMENTED`.

## Phase 1: Planning (`get_flight_info_statement`)

When a client sends `SELECT avg(amount) FROM orders`, it arrives as a `CommandStatementQuery` in `get_flight_info_statement`:

```rust
async fn get_flight_info_statement(
    &self,
    query: CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    // Parse SQL text to AST
    let statement = parse_one_statement(&query.query)
        .map_err(|e| Status::invalid_argument(format!("parse error: {e}")))?;

    // Convert AST to spec::Plan IR
    let plan = from_ast_statement(statement)
        .map_err(|e| Status::invalid_argument(format!("plan conversion error: {e}")))?;

    // Get (or create) the session context
    let ctx = self.get_session_context().await?;

    // Resolve, optimize, and create a physical plan; get back a stream handle
    let (plan, _) = resolve_and_execute_plan(&ctx, self.config.clone(), plan)
        .await
        .map_err(|e| Status::internal(format!("plan error: {e}")))?;

    let schema = plan.schema();
    let service = ctx.extension::<JobService>()?;
    let stream = service.runner().execute(&ctx, plan).await?;

    // Wrap stream in metrics if enabled
    let stream = if let Some(ref m) = self.metrics {
        Box::pin(MetricsRecordingStream::new(stream, m.clone(), ctx))
    } else {
        stream
    };

    // Store the stream under an opaque handle
    let handle = QueryHandle::new();
    self.state.lock().await.add_stream(handle.clone(), stream);

    // Package the handle as a Flight Ticket
    let ticket = TicketStatementQuery {
        statement_handle: handle.as_bytes().to_vec().into(),
    };
    let ticket_bytes = ticket.as_any().encode_to_vec();

    // Return FlightInfo with the schema and the ticket
    let endpoint = FlightEndpoint {
        ticket: Some(Ticket { ticket: ticket_bytes.into() }),
        location: vec![],
        expiration_time: None,
        app_metadata: Default::default(),
    };
    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(request.into_inner())
        .try_with_schema(&schema)?;

    Ok(Response::new(info))
}
```

The notable design decision: Sail starts execution *eagerly* in `get_flight_info_statement`. The `service.runner().execute(...)` call creates the `SendableRecordBatchStream` and spawns the execution. The stream is stored in `SailFlightSqlState` (a `HashMap<QueryHandle, SendableRecordBatchStream>` behind a `Mutex<...>`), keyed by the handle. The client gets back the schema and a ticket immediately; the query is already running.

This is different from the Spark Connect path, where execution starts only when the client calls `ExecutePlan` and begins consuming the streaming response. Flight SQL's two-phase design means execution must start at phase 1, because the `FlightInfo` cannot include the output schema without having resolved the plan.

## Phase 2: Streaming (`do_get_statement`)

When the client presents the ticket, it calls `DoGet`:

```rust
async fn do_get_statement(
    &self,
    ticket: TicketStatementQuery,
    _request: Request<Ticket>,
) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    let handle = QueryHandle::try_from(ticket.statement_handle.as_ref())?;

    // Remove the stream from the state map (consume once)
    let stream = self
        .state
        .lock()
        .await
        .remove_stream(&handle)
        .ok_or_else(|| Status::not_found(
            format!("query handle not found or already consumed: {handle}")
        ))?;

    let schema = stream.schema();

    // Convert RecordBatch errors to FlightError
    let output = stream.map(|result| {
        result.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))
    });

    // Encode as Arrow IPC Flight frames
    let output = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(output)
        .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

    Ok(Response::new(Box::pin(output)))
}
```

`FlightDataEncoderBuilder` from `arrow-flight` handles the encoding: it takes a `Stream<Item = Result<RecordBatch, ArrowError>>` and produces a `Stream<Item = Result<FlightData, FlightError>>`. Each `FlightData` message is an Arrow IPC frame that the client can decode directly with `pyarrow` or any ADBC driver.

The `remove_stream` semantics — the handle is consumed once — mean that each query can only be fetched once. If the client's network fails after the ticket is issued but before `DoGet` is called, the query is lost. This is acceptable for the Flight SQL use case (re-execute on failure) and avoids indefinite server-side memory growth.

## The Session Model for Flight SQL

Unlike Spark Connect, which creates one `SessionContext` per client session, the Flight SQL service uses a single shared session:

```rust
impl SailFlightSqlService {
    const DEFAULT_SESSION_ID: &'static str = "flight-default";
    const DEFAULT_USER_ID: &'static str = "flight-user";

    async fn get_session_context(&self) -> Result<SessionContext, Status> {
        self.session_manager
            .get_or_create_session_context(
                Self::DEFAULT_SESSION_ID.to_string(),
                Self::DEFAULT_USER_ID.to_string(),
            )
            .await
            .map_err(|e| Status::internal(format!("session error: {e}")))
    }
}
```

This is a deliberate simplification for the initial Flight SQL implementation. All Flight SQL queries share one session, which means they share the same configuration and catalog state. A future version could multiplex sessions using the `CallHeaders` mechanism from Flight (which can carry authentication tokens or session IDs).

## Handshake

Flight's `DoHandshake` is used for authentication. Sail's implementation is minimal:

```rust
async fn do_handshake(
    &self,
    _request: Request<Streaming<HandshakeRequest>>,
) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status>
{
    debug!("handshake received from client");
    let response = HandshakeResponse {
        protocol_version: 0,
        payload: Default::default(),
    };
    let output = stream::iter(vec![Ok(response)]);
    Ok(Response::new(Box::pin(output)))
}
```

The `// Note: not all clients perform handshake with the server.` comment in the source is informative: some Flight SQL clients (e.g. ADBC with certain drivers) skip the handshake and go directly to `GetFlightInfo`. Sail accepts both patterns.

## Metrics and Observability

When OpenTelemetry is enabled at server startup, the Flight SQL service wraps result streams with `MetricsRecordingStream`:

```rust
let stream: SendableRecordBatchStream = if let Some(ref m) = self.metrics {
    Box::pin(MetricsRecordingStream::new(
        stream,
        m.clone(),
        MetricsRecordingContext { statement_type },
    ))
} else {
    stream
};
```

`MetricsRecordingStream` is a transparent wrapper that tracks row counts, batch counts, and elapsed time, recording them to the OpenTelemetry `MetricRegistry` when the stream is fully consumed or dropped. The `StatementType` discriminates between `Query` (SELECT) and `Command` (DDL, DML) for metric labels.

## Command Execution

Flight SQL commands (DDL, INSERT, etc.) need special handling because they produce no rows. Sail handles this by eagerly draining the stream and returning an empty result:

```rust
let stream: SendableRecordBatchStream = match statement_type {
    StatementType::Query => stream,
    StatementType::Command => {
        // Execute command eagerly and store the result in memory
        let mut stream = stream;
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result.map_err(|e| Status::internal(...))?);
        }
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(batches.into_iter().map(Ok)),
        ))
    }
};
```

This ensures that DDL commands run to completion before `get_flight_info_statement` returns, so the client can trust that the command succeeded when it receives the `FlightInfo`. The result stream in state will be empty (zero batches) but still present, which the client can fetch or ignore.

## Comparison: Flight SQL vs Spark Connect

| Aspect | Spark Connect | Flight SQL |
|---|---|---|
| Protocol | Spark-specific protobuf | Arrow Flight RPC standard |
| Session model | Per-session state, configurable | Single shared session |
| Streaming | Native reattachable streaming | Two-phase (info + fetch) |
| Execution start | On first stream poll | During `GetFlightInfo` |
| Error reattach | `ReattachExecute` RPC | Re-execute |
| Primary use | PySpark / pyspark-client | ADBC, JDBC, BI tools |

## Summary

`sail-flight` provides an Arrow Flight SQL entry point alongside Spark Connect. Its implementation follows the standard two-phase pattern: plan eagerly in `get_flight_info_statement`, stream results in `do_get_statement`. Execution starts at phase 1; the resulting stream is stored in a handle map and consumed once when the client fetches it. This makes Sail accessible to any Flight SQL-compatible client — DuckDB, Tableau, Apache Superset — without requiring PySpark.
