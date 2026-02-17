# ADR-002: gRPC for Inter-Service Communication

## Status
Accepted

## Context

Microservices need to communicate synchronously for operations like:
- BFF querying Ticket Service for ticket details
- Routing Service calling AI Service for ticket analysis
- Ticket Service calling User Service to validate agent existence

We need to choose a protocol for this synchronous communication. Options include:
- REST (HTTP/JSON)
- gRPC (HTTP/2 + Protobuf)
- GraphQL
- Plain HTTP with custom serialization

## Decision

We will use **gRPC** for synchronous inter-service communication.

**Protocol details:**
- Protobuf for message serialization
- HTTP/2 for transport
- Unary calls for request-response
- Server streaming for AI suggestions (real-time responses)
- Client → BFF uses REST/JSON (Next.js API Routes)

**Example contract:**

```protobuf
service TicketService {
  rpc CreateTicket(CreateTicketRequest) returns (Ticket);
  rpc GetTicket(GetTicketRequest) returns (Ticket);
  rpc ListTickets(ListTicketsRequest) returns (ListTicketsResponse);
}

service AIService {
  rpc AnalyzeTicket(AnalyzeTicketRequest) returns (AnalyzeTicketResponse);
  rpc GetSuggestions(GetSuggestionsRequest) returns (stream SuggestionChunk);
}
```

## Consequences

### Positive

**Performance:**
- Binary serialization (Protobuf) is faster than JSON
- HTTP/2 multiplexing reduces connection overhead
- Smaller payload size (~30% vs JSON)

**Type Safety:**
- Strongly-typed contracts via `.proto` files
- Code generation for C# clients/servers
- Compile-time checks prevent contract drift

**Streaming Support:**
- Server streaming for AI suggestions (send tokens as generated)
- Bidirectional streaming for future use cases

**Developer Experience:**
- Auto-generated documentation from `.proto` files
- gRPC reflection for debugging (grpcurl)
- Consistent error handling via status codes

### Negative

**Debugging Difficulty:**
- Binary format not human-readable
- Requires tools like grpcurl or BloomRPC
- Can't use simple curl commands

**Tooling:**
- Requires code generation step in build pipeline
- More setup compared to REST

**Browser Compatibility:**
- gRPC-Web required for browser clients
- We solve this with BFF (clients → REST → BFF → gRPC)

### Risks

**Learning Curve:**
- Team must learn Protobuf syntax
- gRPC error handling differs from REST

**Mitigation:**
- Create Skills document after first gRPC implementation
- Use gRPC interceptors for common concerns (logging, auth)

**Breaking Changes:**
- Protobuf schema changes can break clients

**Mitigation:**
- Follow backward-compatible evolution rules
- Never remove/rename fields, only add with new field numbers

## Alternatives Considered

### Alternative 1: REST (HTTP/JSON)

**Pros:**
- Universal standard, everyone knows it
- Human-readable (easy debugging with curl)
- Extensive tooling (Postman, Swagger)
- No code generation needed

**Cons:**
- JSON parsing overhead (~20-30% slower than Protobuf)
- No type safety (runtime errors for schema mismatches)
- No native streaming (must use SSE or polling)
- Larger payload sizes

**Rejected because:** 
- Performance matters for high-frequency calls (BFF → services)
- Streaming needed for AI suggestions
- Type safety reduces bugs in distributed system

### Alternative 2: GraphQL

**Pros:**
- Flexible queries (client specifies fields)
- Single endpoint
- Built-in introspection

**Cons:**
- Primarily client-server, not service-to-service
- No streaming (must use subscriptions)
- Over-fetching/under-fetching less relevant for internal services
- Overkill for our use case

**Rejected because:** 
- GraphQL is better for client-facing APIs
- We have BFF for client orchestration
- Internal services have well-defined contracts, don't need query flexibility

### Alternative 3: Message Queue Only (RabbitMQ/Kafka)

**Pros:**
- Fully asynchronous (no blocking)
- Natural decoupling

**Cons:**
- Request-response pattern requires correlation IDs
- Latency (wait for message to be consumed)
- Complexity for simple queries (e.g., "get ticket by ID")

**Rejected because:**
- Synchronous queries are natural (BFF needs ticket details NOW)
- Async should be reserved for truly async operations (events, tasks)

## Implementation Guidelines

**Project Structure:**
```
TicketFlow.Contracts/
  ├── Protos/
  │   ├── ticket.proto
  │   ├── user.proto
  │   ├── ai.proto
  │   └── common.proto
  └── TicketFlow.Contracts.csproj
```

**Service Registration:**
```csharp
// Server (Ticket Service)
services.AddGrpc();
app.MapGrpcService<TicketServiceImpl>();

// Client (BFF)
services.AddGrpcClient<TicketService.TicketServiceClient>(options =>
{
    options.Address = new Uri("https://ticket-service:5002");
});
```

**Error Handling:**
```csharp
// Server throws
throw new RpcException(new Status(
    StatusCode.NotFound, 
    "Ticket not found"
));

// Client catches
try {
    var ticket = await client.GetTicketAsync(request);
} catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound) {
    return NotFound();
}
```

**Multitenancy:**
```csharp
// Client adds tenant_id to metadata
var metadata = new Metadata {
    { "tenant-id", tenantId }
};
await client.GetTicketAsync(request, metadata);

// Server interceptor extracts
public class TenantInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, 
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var tenantId = context.RequestHeaders.GetValue("tenant-id");
        _tenantContext.TenantId = tenantId;
        return await continuation(request, context);
    }
}
```

## Related Decisions

- ADR-001: Microservices architecture (requires inter-service communication)
- ADR-006: Next.js BFF (uses REST for client, gRPC for services)
- ADR-008: WebSocket for real-time (gRPC streaming not suitable for browsers)

## References

- [gRPC Documentation](https://grpc.io/docs/)
- [gRPC Performance Best Practices](https://grpc.io/docs/guides/performance/)
- [Protocol Buffers Language Guide](https://protobuf.dev/programming-guides/proto3/)

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
