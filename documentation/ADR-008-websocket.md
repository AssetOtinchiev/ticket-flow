# ADR-008: WebSocket for Real-Time Communication

## Status
Accepted

## Context

The system requires real-time features:

**Chat (US-007):**
- Agent and customer exchange messages
- See "typing..." indicator
- Message read receipts
- Instant delivery (not polling)

**Live Updates (US-005):**
- Inbox refreshes when new ticket arrives
- Ticket status changes appear immediately
- Notifications (new assignment, escalation)

**Options for real-time communication:**

1. **HTTP Polling** (client requests every N seconds)
   - Simple
   - High latency (N seconds delay)
   - Wasteful (empty responses if no updates)

2. **HTTP Long Polling** (server holds request until data available)
   - Lower latency than polling
   - Still creates many connections

3. **Server-Sent Events (SSE)** (server pushes to client)
   - Unidirectional (server → client only)
   - Simple protocol
   - Limited browser support

4. **WebSocket** (bidirectional, persistent connection)
   - Full duplex (both directions)
   - Low latency (<100ms)
   - Efficient (one connection, no HTTP overhead per message)

We need to choose a real-time protocol.

## Decision

We will use **WebSocket** for real-time communication.

**Implementation:**
```
Browser Client (WebSocket client)
    ↓ WSS (WebSocket Secure)
Next.js BFF (WebSocket proxy)
    ↓ Internal WebSocket or gRPC streaming
Chat Service (WebSocket server)
```

**Protocol:**
- Connection: `wss://app.ticketflow.com/ws?token={jwt}`
- Authentication: JWT in query param or first message
- Message format: JSON
- Channels: `ticket.{id}`, `inbox.{userId}`, `department.{deptId}`

**Scalability via Redis Pub/Sub:**
```
Agent A → WebSocket Server 1 → Redis Pub/Sub → WebSocket Server 2 → Agent B
```

Multiple WebSocket servers share state through Redis. When a message is sent, it's published to Redis, and all servers forward it to their connected clients.

## Consequences

### Positive

**Low Latency:**
- <100ms message delivery (no polling delay)
- Instant notifications (typing, status changes)
- Real-time experience (feels responsive)

**Efficiency:**
- Single persistent connection (no repeated HTTP handshakes)
- No empty polling responses
- Lower bandwidth (no HTTP headers per message)

**Bidirectional:**
- Client can send (chat messages, typing indicator)
- Server can push (new messages, notifications)
- Same connection for both directions

**Standard Protocol:**
- Native browser support (no libraries needed)
- Well-understood (many resources, tools)
- Production-proven (Slack, Discord use WebSocket)

### Negative

**Connection Management:**
- Must handle disconnects, reconnects
- Heartbeat/ping-pong to detect dead connections
- Stale connections consume resources

**Scalability Complexity:**
- Stateful (server remembers connected clients)
- Can't just add servers (need shared state)
- Requires Redis pub/sub for multi-server setup

**Load Balancer Configuration:**
- Need sticky sessions or connection upgrade support
- Not all load balancers handle WebSocket well

**Debugging:**
- Harder to debug than HTTP (no curl)
- Need specialized tools (wscat, browser devtools)

### Risks

**Connection Storms:**
- 1,000 agents reconnecting simultaneously (server restart)

**Mitigation:**
- Exponential backoff on client (1s, 2s, 4s, 8s)
- Connection rate limiting (max 100 new connections/sec)

**Memory Leaks:**
- Forgotten connections accumulate

**Mitigation:**
- Heartbeat every 30s (disconnect if no pong for 60s)
- Track active connections, alert if > threshold

**Redis as SPOF:**
- If Redis fails, cross-server messages stop

**Mitigation:**
- Redis Sentinel for high availability
- Fallback: Direct database polling (degraded mode)

## Alternatives Considered

### Alternative 1: HTTP Polling

**Code:**
```typescript
// Client polls every 5 seconds
setInterval(async () => {
  const response = await fetch('/api/messages?ticketId=123&since=' + lastMessageId);
  const newMessages = await response.json();
  if (newMessages.length > 0) {
    updateUI(newMessages);
  }
}, 5000);
```

**Pros:**
- Simplest to implement
- Works everywhere (no special load balancer config)
- Stateless (easy to scale)

**Cons:**
- High latency (up to 5 seconds delay)
- Wasteful (empty responses 90% of the time)
- Poor UX (not really "real-time")

**Rejected because:**
- Latency unacceptable for chat (feels slow)
- Inefficient (1,000 agents × 12 polls/min = 12K requests/min doing nothing)

### Alternative 2: Server-Sent Events (SSE)

**Code:**
```typescript
// Client
const eventSource = new EventSource('/api/messages/stream?ticketId=123');
eventSource.onmessage = (event) => {
  const message = JSON.parse(event.data);
  appendMessage(message);
};
```

**Pros:**
- Simpler than WebSocket (built on HTTP)
- Automatic reconnect
- Server push (low latency)

**Cons:**
- Unidirectional (server → client only)
- Client must use HTTP POST for sending messages
- Limited browser support (IE not supported)
- Connection limits (6 per domain in some browsers)

**Rejected because:**
- Need bidirectional (typing indicator from client)
- Chat requires both send and receive
- WebSocket better supported and more flexible

### Alternative 3: gRPC Streaming (Client → BFF)

**Code:**
```typescript
const stream = grpcClient.streamMessages({ ticketId: '123' });
stream.on('data', (message) => {
  appendMessage(message);
});
```

**Pros:**
- Bidirectional streaming
- Type-safe (Protobuf)

**Cons:**
- gRPC-Web required (not native browser support)
- More complex setup (proxy needed)
- Overkill for simple chat

**Rejected because:**
- WebSocket native in browsers
- gRPC better for service-to-service
- Chat doesn't need Protobuf type safety

### Alternative 4: HTTP/2 Server Push

**Pros:**
- Modern protocol
- Multiplexing

**Cons:**
- Server push deprecated in Chrome
- Not designed for long-lived connections
- Worse support than WebSocket

**Rejected because:**
- Being phased out by browsers
- WebSocket purpose-built for this use case

## Implementation Details

### WebSocket Server (Chat Service)

```csharp
// ASP.NET Core WebSocket endpoint
app.UseWebSockets();
app.Use(async (context, next) =>
{
    if (context.Request.Path == "/ws")
    {
        if (context.WebSockets.IsWebSocketRequest)
        {
            var webSocket = await context.WebSockets.AcceptWebSocketAsync();
            await HandleWebSocketAsync(context, webSocket);
        }
        else
        {
            context.Response.StatusCode = 400;
        }
    }
    else
    {
        await next();
    }
});

async Task HandleWebSocketAsync(HttpContext context, WebSocket webSocket)
{
    // Authenticate
    var token = context.Request.Query["token"];
    var claims = ValidateJwt(token);
    var userId = claims.FindFirst("sub").Value;
    var tenantId = claims.FindFirst("tenant_id").Value;

    // Register connection
    var connectionId = Guid.NewGuid().ToString();
    _connections.TryAdd(connectionId, new Connection
    {
        WebSocket = webSocket,
        UserId = userId,
        TenantId = tenantId,
        Channels = new HashSet<string>()
    });

    // Receive loop
    var buffer = new byte[1024 * 4];
    WebSocketReceiveResult result = await webSocket.ReceiveAsync(
        new ArraySegment<byte>(buffer), CancellationToken.None);

    while (!result.CloseStatus.HasValue)
    {
        var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
        await HandleMessageAsync(connectionId, message);

        result = await webSocket.ReceiveAsync(
            new ArraySegment<byte>(buffer), CancellationToken.None);
    }

    // Cleanup
    _connections.TryRemove(connectionId, out _);
    await webSocket.CloseAsync(
        result.CloseStatus.Value, 
        result.CloseStatusDescription, 
        CancellationToken.None);
}
```

### WebSocket Client (Next.js)

```typescript
// lib/websocket.ts
export class WebSocketClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;

  connect(token: string) {
    const wsUrl = `${process.env.NEXT_PUBLIC_WS_URL}/ws?token=${token}`;
    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      console.log('WebSocket connected');
      this.reconnectAttempts = 0;
      
      // Subscribe to channels
      this.send({
        type: 'subscribe',
        channels: ['inbox.user-123', 'ticket.456']
      });

      // Start heartbeat
      this.startHeartbeat();
    };

    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      this.handleMessage(message);
    };

    this.ws.onclose = () => {
      console.log('WebSocket disconnected');
      this.reconnect();
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  send(message: any) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }

  private reconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
      this.reconnectAttempts++;
      
      console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
      setTimeout(() => this.connect(this.token), delay);
    }
  }

  private startHeartbeat() {
    setInterval(() => {
      this.send({ type: 'ping' });
    }, 30000);
  }

  private handleMessage(message: any) {
    switch (message.type) {
      case 'message.new':
        // Update chat UI
        break;
      case 'ticket.status_changed':
        // Update ticket list
        break;
      case 'pong':
        // Heartbeat response
        break;
    }
  }
}
```

### Redis Pub/Sub for Multi-Server

```csharp
// Chat Service
public class WebSocketManager
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ConcurrentDictionary<string, Connection> _connections;

    public async Task SendToChannel(string channel, object message)
    {
        var json = JsonSerializer.Serialize(message);
        
        // Publish to Redis (all servers receive)
        await _redis.GetSubscriber().PublishAsync(channel, json);
    }

    public void StartRedisSubscription()
    {
        var subscriber = _redis.GetSubscriber();

        // Subscribe to all channels this server cares about
        subscriber.Subscribe("ticket.*", (channel, message) =>
        {
            var msg = JsonSerializer.Deserialize<Message>(message);
            
            // Find local connections subscribed to this channel
            foreach (var conn in _connections.Values)
            {
                if (conn.Channels.Contains(channel))
                {
                    SendToWebSocket(conn.WebSocket, message);
                }
            }
        });
    }
}
```

### Message Protocol

```typescript
// Client → Server
{
  "type": "subscribe",
  "channels": ["ticket.123", "inbox.user-456"]
}

{
  "type": "message.send",
  "ticket_id": "123",
  "content": "Hello, how can I help?"
}

{
  "type": "typing",
  "ticket_id": "123",
  "is_typing": true
}

// Server → Client
{
  "type": "message.new",
  "ticket_id": "123",
  "message": {
    "id": "msg-789",
    "sender_id": "user-456",
    "content": "I need help with...",
    "created_at": 1634567890
  }
}

{
  "type": "ticket.status_changed",
  "ticket_id": "123",
  "old_status": "OPEN",
  "new_status": "IN_PROGRESS",
  "changed_by": "agent-456"
}

{
  "type": "typing",
  "ticket_id": "123",
  "user_id": "agent-456",
  "is_typing": true
}

// Heartbeat
{
  "type": "ping"
}
// Expected response
{
  "type": "pong"
}
```

## Testing

**Integration Test:**
```csharp
[Fact]
public async Task WebSocket_ShouldDeliverMessage()
{
    // Arrange
    var client = new ClientWebSocket();
    await client.ConnectAsync(new Uri("ws://localhost:5003/ws?token=" + token), CancellationToken.None);

    // Subscribe
    await SendAsync(client, new { type = "subscribe", channels = new[] { "ticket.123" } });

    // Act
    await _chatService.SendMessageAsync(new SendMessageCommand
    {
        TicketId = "123",
        Content = "Test message"
    });

    // Assert
    var received = await ReceiveAsync(client, TimeSpan.FromSeconds(5));
    received.Should().NotBeNull();
    received.Type.Should().Be("message.new");
    received.Message.Content.Should().Be("Test message");
}
```

## Related Decisions

- ADR-001: Microservices (Chat Service handles WebSocket)
- ADR-006: Next.js BFF (proxies WebSocket connections)

## References

- [WebSocket Protocol RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)
- [ASP.NET Core WebSockets](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/websockets)
- [Scaling WebSocket with Redis](https://redis.io/docs/manual/pubsub/)

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
