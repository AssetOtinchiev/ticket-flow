# ADR-001: Microservices Architecture

## Status
Accepted

## Context

We need to decide on the overall architecture pattern for TicketFlow. The system handles multiple bounded contexts (users, tickets, chat, routing, analytics) with different scaling and development characteristics.

For a system with 10,000 tickets/day and 500 concurrent agents, a **monolithic architecture** would be simpler:
- Single deployment
- No network overhead
- Easier debugging
- Lower operational complexity

However, this is an **educational project** aimed at preparing for System Design interviews at Big Tech companies (Yandex, Google, etc.), where microservices are the norm.

## Decision

We will use **microservices architecture** with the following services:
1. User Service (authentication, teams)
2. Ticket Service (ticket CRUD, status management)
3. Chat Service (real-time messaging)
4. Routing Service (AI-powered assignment rules)
5. SLA Service (deadline monitoring, escalations)
6. AI Service (LLM integration)
7. Integration Service (Jira, external channels)
8. Analytics Service (ClickHouse queries)
9. Next.js BFF (orchestration)

Each service has its own PostgreSQL database (except AI Service and Analytics Service).

## Consequences

### Positive

**Learning Value:**
- Experience with distributed systems patterns (Saga, Outbox, Circuit Breaker)
- Practice with service boundaries and bounded contexts
- System Design interview preparation (able to discuss trade-offs)
- Understanding of eventual consistency vs strong consistency

**Scalability:**
- Services can scale independently based on load
- Teams can deploy independently (in production)
- Technology heterogeneity (future: different languages per service)

**Fault Isolation:**
- Failure in AI Service doesn't bring down ticket creation
- Can implement graceful degradation per service

### Negative

**Complexity:**
- Distributed transactions (compensating actions)
- Network latency between services
- More difficult debugging (correlation IDs required)
- Higher operational overhead (9 services to monitor)

**Development Speed:**
- Slower initial development (setup infrastructure)
- More boilerplate (shared libraries, contracts)
- Testing complexity (integration tests with multiple services)

### Risks

**Over-engineering:**
- For 10K tickets/day, a monolith would suffice
- Risk of premature optimization

**Mitigation:** 
- Accept this as an **educational trade-off**
- Document that this is intentionally complex for learning
- In real world, start with monolith → split later when needed

## Alternatives Considered

### Alternative 1: Monolithic Application

**Pros:**
- Simpler deployment (single artifact)
- Easier local development
- No network overhead
- Transactions are ACID by default

**Cons:**
- Less relevant for System Design interviews
- No experience with distributed patterns
- Harder to demonstrate service boundaries

**Rejected because:** Primary goal is System Design interview preparation, not production optimization.

### Alternative 2: Modular Monolith

**Pros:**
- Clear module boundaries (preparation for microservices)
- Simpler deployment
- Can split into microservices later

**Cons:**
- Still doesn't provide distributed systems experience
- Can't demonstrate Kafka/gRPC integration patterns

**Rejected because:** Doesn't meet learning objectives (distributed patterns, inter-service communication).

### Alternative 3: Serverless (AWS Lambda + API Gateway)

**Pros:**
- Auto-scaling
- Pay-per-use
- No infrastructure management

**Cons:**
- Vendor lock-in
- Limited to AWS knowledge
- Doesn't translate to on-premise Big Tech setups
- Cold start latency

**Rejected because:** We want transferable knowledge applicable to any company, not cloud-specific.

## Implementation Notes

**Service Communication:**
- gRPC for synchronous calls (type-safe, fast)
- Kafka for asynchronous events (audit log, analytics)
- RabbitMQ for task queues (email, integrations)
- WebSocket for real-time (chat, notifications)

**Data Management:**
- Database per service (PostgreSQL)
- Shared ClickHouse for analytics (read-only)
- No shared database between services

**Deployment:**
- Docker Compose for local development
- Each service in separate container
- Shared network for inter-service communication

## Related Decisions

- ADR-002: gRPC for inter-service communication
- ADR-003: Kafka for event streaming
- ADR-005: ClickHouse for analytics (OLAP separation)

## References

- [Microservices Patterns by Chris Richardson](https://microservices.io/patterns/)
- [Building Microservices by Sam Newman](https://www.oreilly.com/library/view/building-microservices-2nd/9781492034018/)
- System Design interview resources focusing on distributed systems

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
