# ADR-006: Next.js as Backend for Frontend (BFF)

## Status
Accepted

## Context

The frontend needs to interact with multiple microservices:

**Example: Agent views ticket #123**
- Ticket details (Ticket Service)
- Message history (Chat Service)
- AI suggestions (AI Service - streaming)
- Customer profile (User Service)
- Department info (User Service)

**Options for handling this:**

1. **Client makes 5 separate API calls**
   - Waterfalls (sequential) or all parallel
   - Client handles errors, retries, timeouts
   - CORS configuration for all services

2. **API Gateway aggregates** (e.g., ASP.NET Gateway)
   - Separate service
   - Duplicates orchestration logic
   - Another deployment to manage

3. **Backend for Frontend (BFF) in Next.js**
   - API Routes handle aggregation
   - Same codebase as frontend
   - Server Components for SSR

We need to decide: Where does orchestration logic live?

## Decision

We will use **Next.js 14+ App Router** as the BFF layer.

**Architecture:**
```
Browser Client
    ↓ HTTPS / WebSocket
Next.js (Port 3000)
    ├─ API Routes (/app/api/**/route.ts) - BFF orchestration
    ├─ Server Components - SSR with direct gRPC calls
    ├─ Client Components - Interactive UI
    └─ WebSocket proxy to Chat Service
    ↓ gRPC (internal network)
Microservices (User, Ticket, Chat, AI, etc.)
```

**BFF Responsibilities:**
- Aggregate data from multiple services
- Transform gRPC responses to JSON for client
- Handle authentication (JWT validation, refresh)
- Server-side caching (Redis, in-memory)
- Rate limiting
- WebSocket proxy for chat

**What BFF does NOT do:**
- Business logic (belongs in domain services)
- Direct database access (except replica PostgreSQL for reads)

## Consequences

### Positive

**Single Codebase:**
- Frontend + BFF in one repository
- Shared TypeScript types (no drift)
- Easier refactoring

**Server-Side Rendering:**
- Initial page load faster (Server Components fetch data)
- SEO-friendly (if needed)
- Reduced client bundle size

**Performance:**
- Parallel gRPC calls on server (faster than sequential HTTP from browser)
- No CORS overhead
- gRPC (binary) faster than REST (JSON)

**Developer Experience:**
- No separate Gateway project
- Hot reload for both frontend and BFF
- Simplified deployment (single artifact)

**Caching:**
- Server-side cache (Redis) shared across requests
- Replica PostgreSQL for fast reads (user profiles, tenant settings)

### Negative

**Coupling:**
- BFF tied to frontend framework (can't easily switch from Next.js)
- If we later build mobile app, might need separate BFF or duplicate logic

**Node.js Performance:**
- Node.js less efficient than .NET for CPU-heavy tasks
- But: BFF mostly I/O (gRPC calls), so Node.js fine

**Testing Complexity:**
- Need to test both frontend logic and BFF orchestration
- More mocking required (gRPC clients)

### Risks

**BFF Becomes Fat:**
- Risk: Business logic creeps into BFF

**Mitigation:**
- Code review: Reject any business rules in BFF
- BFF only orchestrates, never decides

**Single Point of Failure:**
- If Next.js crashes, entire UI down

**Mitigation:**
- Run multiple instances behind load balancer
- Health checks, auto-restart

## Alternatives Considered

### Alternative 1: Client Calls Services Directly

**Example:**
```typescript
// Client-side code
const [ticket, setTicket] = useState(null);
const [messages, setMessages] = useState([]);

useEffect(() => {
  Promise.all([
    fetch('/api/tickets/123'),
    fetch('/api/messages?ticketId=123'),
    fetch('/api/ai/suggestions?ticketId=123')
  ]).then(([t, m, s]) => {
    setTicket(t);
    setMessages(m);
    setSuggestions(s);
  });
}, []);
```

**Pros:**
- Simpler architecture (no BFF)
- Direct connection to services

**Cons:**
- Multiple HTTP requests (latency)
- Client must handle errors, retries
- Exposes internal service structure to client
- CORS complexity
- Can't use gRPC (browser limitation)

**Rejected because:**
- Performance: 3 sequential HTTP calls slower than 1 with server-side parallel gRPC
- Security: Internal service URLs exposed
- Complexity: Error handling duplicated in client

### Alternative 2: Separate ASP.NET Gateway

**Example:**
```csharp
// Gateway.API/Controllers/TicketAggregateController.cs
[HttpGet("tickets/{id}")]
public async Task<TicketAggregateResponse> GetTicketAggregate(string id)
{
    var (ticket, messages, suggestions) = await Task.WhenAll(
        _ticketClient.GetTicketAsync(new GetTicketRequest { Id = id }),
        _chatClient.GetMessagesAsync(new GetMessagesRequest { TicketId = id }),
        _aiClient.GetSuggestionsAsync(new GetSuggestionsRequest { TicketId = id })
    );
    
    return new TicketAggregateResponse { Ticket = ticket, Messages = messages, Suggestions = suggestions };
}
```

**Pros:**
- Language consistency (all C#)
- Independent deployment from frontend
- Reusable for mobile apps

**Cons:**
- Separate deployment (more operational overhead)
- Duplicates orchestration logic (Gateway + Next.js for SSR)
- More code to maintain

**Rejected because:**
- Next.js can do the same with API Routes
- Don't want two orchestration layers
- Simplicity: fewer moving parts

### Alternative 3: GraphQL Gateway (Apollo Federation)

**Pros:**
- Flexible queries (client specifies fields)
- Single endpoint
- Schema stitching (combine services)

**Cons:**
- Overkill for internal APIs
- Learning curve (GraphQL)
- Microservices already have well-defined contracts (gRPC)
- Over-fetching/under-fetching not a concern (internal)

**Rejected because:**
- GraphQL better for public APIs with diverse clients
- We have full control over client and services
- gRPC already provides type safety

## Implementation Details

### API Route Example (Aggregation)

```typescript
// app/api/tickets/[id]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getSession } from '@/lib/auth';
import { ticketClient, chatClient, aiClient, userClient } from '@/lib/grpc-clients';

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  const session = await getSession();
  if (!session) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const metadata = {
    'tenant-id': session.tenantId,
    'user-id': session.userId,
  };

  try {
    // Parallel gRPC calls
    const [ticket, messages, suggestions, customer] = await Promise.allSettled([
      ticketClient.getTicket({ id: params.id }, { metadata }),
      chatClient.getMessages({ ticketId: params.id }, { metadata }),
      aiClient.getSuggestions({ ticketId: params.id }, { metadata }), // streaming handled separately
      userClient.getUser({ id: ticket.customerId }, { metadata }),
    ]);

    // Graceful degradation: If AI Service fails, still return ticket
    return NextResponse.json({
      ticket: ticket.status === 'fulfilled' ? ticket.value : null,
      messages: messages.status === 'fulfilled' ? messages.value : [],
      suggestions: suggestions.status === 'fulfilled' ? suggestions.value : null,
      customer: customer.status === 'fulfilled' ? customer.value : null,
    });
  } catch (error) {
    console.error('BFF aggregation error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
```

### Server Component Example (SSR)

```tsx
// app/tickets/[id]/page.tsx (Server Component)
import { ticketClient, chatClient } from '@/lib/grpc-clients';
import { getSession } from '@/lib/auth';

export default async function TicketPage({ params }: { params: { id: string } }) {
  const session = await getSession();
  const metadata = { 'tenant-id': session.tenantId };

  // Direct gRPC calls in Server Component
  const [ticket, messages] = await Promise.all([
    ticketClient.getTicket({ id: params.id }, { metadata }),
    chatClient.getMessages({ ticketId: params.id }, { metadata }),
  ]);

  return (
    <div>
      <TicketHeader ticket={ticket} />
      <MessageList messages={messages} />
      <AISuggestions ticketId={ticket.id} /> {/* Client Component for streaming */}
    </div>
  );
}
```

### Caching Layer (Redis)

```typescript
// lib/cache.ts
import { createClient } from 'redis';

const redis = createClient({ url: process.env.REDIS_URL });

export async function getCachedUser(userId: string) {
  const cached = await redis.get(`user:${userId}`);
  if (cached) return JSON.parse(cached);

  // Cache miss: fetch from User Service
  const user = await userClient.getUser({ id: userId });
  await redis.set(`user:${userId}`, JSON.stringify(user), { EX: 900 }); // 15 min TTL
  return user;
}

// Invalidate on update (via Kafka event)
kafkaConsumer.on('user.updated', async (event) => {
  await redis.del(`user:${event.userId}`);
});
```

### Replica PostgreSQL for Fast Reads

```typescript
// lib/replica-db.ts
import { Pool } from 'pg';

const replicaPool = new Pool({
  host: process.env.POSTGRES_REPLICA_HOST,
  database: 'ticketflow',
  user: 'readonly',
  password: process.env.POSTGRES_REPLICA_PASSWORD,
});

// BFF can query replica for heavy reads (e.g., customer profile with full history)
export async function getCustomerProfile(customerId: string, tenantId: string) {
  const result = await replicaPool.query(
    `SELECT u.*, 
            COUNT(t.id) as total_tickets,
            AVG(t.rating) as avg_rating
     FROM users u
     LEFT JOIN tickets t ON t.customer_id = u.id
     WHERE u.id = $1 AND u.tenant_id = $2
     GROUP BY u.id`,
    [customerId, tenantId]
  );
  return result.rows[0];
}
```

## Guidelines: When to Use BFF vs Direct Service Call

### Use BFF API Route when:
- Aggregating data from multiple services
- Client needs REST/JSON (browser)
- Need server-side caching
- Authentication/authorization required

### Use Server Component with direct gRPC when:
- SSR page load
- No client-side interaction needed
- Data only used for rendering

### Use Client Component with API Route when:
- Interactive features (forms, clicks)
- Real-time updates (WebSocket)
- User-triggered actions

## Related Decisions

- ADR-001: Microservices (BFF orchestrates them)
- ADR-002: gRPC between services (BFF uses gRPC internally)
- ADR-004: WebSocket for real-time (BFF proxies to Chat Service)

## References

- [Backend for Frontend Pattern](https://samnewman.io/patterns/architectural/bff/)
- [Next.js App Router](https://nextjs.org/docs/app)
- [Next.js Server Components](https://nextjs.org/docs/app/building-your-application/rendering/server-components)

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
