# ADR-007: Transactional Outbox Pattern

## Status
Accepted

## Context

Services need to publish events to Kafka when data changes:

**Example: Ticket created**
```csharp
// Ticket Service
var ticket = new Ticket { /* ... */ };
await _context.Tickets.AddAsync(ticket);
await _context.SaveChangesAsync();  // Write to PostgreSQL

await _kafka.ProduceAsync("ticket.events", new TicketCreatedEvent {
    TicketId = ticket.Id,
    /* ... */
});  // Publish to Kafka
```

**Problem: Dual Write**

This code has two writes to different systems (PostgreSQL and Kafka). What if:
1. PostgreSQL succeeds, Kafka fails → Event lost (analytics missing data)
2. PostgreSQL fails, Kafka succeeds → Event published for non-existent ticket
3. Network partition between saves → Inconsistent state

This is the **dual-write problem**: No way to make both operations atomic.

**Consequences:**
- Lost events → Analytics incomplete
- Phantom events → Consumers process non-existent tickets
- Retry amplifies problem (duplicate events or database errors)

We need **at-least-once delivery** guarantee: Every database write must result in an event.

## Decision

We will use the **Transactional Outbox Pattern**.

**How it works:**
1. Write to database + outbox table **in same transaction** (atomic)
2. Background job polls outbox table
3. Publish events to Kafka
4. Mark outbox row as processed
5. Delete processed rows (or soft-delete for debugging)

**Schema:**
```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(100) NOT NULL,  -- 'ticket', 'message'
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,      -- 'ticket.created'
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unprocessed 
ON outbox_events(created_at) 
WHERE processed_at IS NULL;
```

## Consequences

### Positive

**Guaranteed Delivery:**
- PostgreSQL transaction includes both business data and event
- If transaction fails, neither is saved
- If transaction succeeds, event will eventually be published

**At-Least-Once Semantics:**
- Events may be published multiple times (if worker crashes after publish but before marking processed)
- Consumers must be idempotent (Kafka consumer offset handles this)
- Better than at-most-once (losing events)

**Audit Trail:**
- Outbox table shows all events
- Can replay events by resetting processed_at to null
- Debugging: See what events were generated

**No Distributed Transaction:**
- Avoids 2PC (Two-Phase Commit)
- PostgreSQL handles ACID, Kafka handles durability separately

### Negative

**Latency:**
- Events not published immediately (polling delay)
- Default: 5 second polling interval
- Trade-off: Lower latency = more database load

**Storage:**
- Outbox table grows (must clean up processed rows)
- Mitigation: Delete rows after N days, or keep for debugging

**Complexity:**
- Background worker required in every service that publishes events
- More code than naive publish

### Risks

**Worker Failure:**
- If worker crashes, events not published

**Mitigation:**
- Worker is BackgroundService (restarts with service)
- Multiple instances can process outbox (partition by tenant_id)

**Outbox Table Bloat:**
- 100K events/day = 36M events/year

**Mitigation:**
- Auto-delete after processing + 7 days (for debugging)
- Partition outbox table by month

**Duplicate Events:**
- Worker may publish same event twice (crash after publish, before marking processed)

**Mitigation:**
- Kafka consumers must be idempotent
- ClickHouse uses ReplacingMergeTree with event_id deduplication

## Implementation

### Write to Outbox (Ticket Service)

```csharp
public class TicketService
{
    private readonly TicketContext _context;

    public async Task<Ticket> CreateTicketAsync(CreateTicketCommand command)
    {
        await using var transaction = await _context.Database.BeginTransactionAsync();

        try
        {
            // 1. Save business entity
            var ticket = new Ticket
            {
                Id = Guid.NewGuid(),
                TenantId = command.TenantId,
                Subject = command.Subject,
                Status = TicketStatus.Open,
                CreatedAt = DateTime.UtcNow
            };
            _context.Tickets.Add(ticket);

            // 2. Save event to outbox (same transaction)
            var outboxEvent = new OutboxEvent
            {
                AggregateType = "ticket",
                AggregateId = ticket.Id,
                EventType = "ticket.created",
                Payload = JsonSerializer.Serialize(new TicketCreatedEvent
                {
                    TicketId = ticket.Id,
                    TenantId = ticket.TenantId,
                    Subject = ticket.Subject,
                    CreatedAt = ticket.CreatedAt,
                    EventId = Guid.NewGuid().ToString()  // for deduplication
                })
            };
            _context.OutboxEvents.Add(outboxEvent);

            // 3. Commit both (atomic)
            await _context.SaveChangesAsync();
            await transaction.CommitAsync();

            return ticket;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
}
```

### Background Worker (Outbox Publisher)

```csharp
public class OutboxPublisher : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<OutboxPublisher> _logger;
    private readonly IKafkaProducer _kafkaProducer;

    public OutboxPublisher(
        IServiceProvider serviceProvider,
        ILogger<OutboxPublisher> logger,
        IKafkaProducer kafkaProducer)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _kafkaProducer = kafkaProducer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Publisher started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOutboxEventsAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox events");
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private async Task ProcessOutboxEventsAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TicketContext>();

        // Fetch unprocessed events (batch of 100)
        var events = await context.OutboxEvents
            .Where(e => e.ProcessedAt == null)
            .OrderBy(e => e.CreatedAt)
            .Take(100)
            .ToListAsync(cancellationToken);

        if (!events.Any())
            return;

        _logger.LogInformation("Processing {Count} outbox events", events.Count);

        foreach (var evt in events)
        {
            try
            {
                // Publish to Kafka
                await _kafkaProducer.ProduceAsync(
                    topic: "ticket.events",
                    key: evt.AggregateId.ToString(),  // Partition by ticket ID
                    value: evt.Payload,
                    cancellationToken
                );

                // Mark as processed
                evt.ProcessedAt = DateTime.UtcNow;

                _logger.LogDebug(
                    "Published event {EventType} for {AggregateType} {AggregateId}",
                    evt.EventType, evt.AggregateType, evt.AggregateId
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to publish event {EventId} of type {EventType}",
                    evt.Id, evt.EventType
                );
                // Don't mark as processed, will retry on next iteration
            }
        }

        // Save processed_at timestamps
        await context.SaveChangesAsync(cancellationToken);
    }
}

// Register in Program.cs
builder.Services.AddHostedService<OutboxPublisher>();
```

### Cleanup Old Events

```csharp
public class OutboxCleanup : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var context = scope.ServiceProvider.GetRequiredService<TicketContext>();

                // Delete events processed more than 7 days ago
                var cutoff = DateTime.UtcNow.AddDays(-7);
                var deleted = await context.OutboxEvents
                    .Where(e => e.ProcessedAt != null && e.ProcessedAt < cutoff)
                    .ExecuteDeleteAsync(stoppingToken);

                if (deleted > 0)
                {
                    _logger.LogInformation("Deleted {Count} old outbox events", deleted);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error cleaning up outbox events");
            }

            // Run once per day
            await Task.Delay(TimeSpan.FromDays(1), stoppingToken);
        }
    }
}
```

## Alternative: Change Data Capture (CDC)

**How it works:**
- Database captures changes from transaction log
- External tool (Debezium) publishes to Kafka
- No application code needed

**Pros:**
- Zero application code (fully automatic)
- Works for legacy apps (no code change)
- Guaranteed ordering (log is sequential)

**Cons:**
- Requires PostgreSQL logical replication
- Debezium is complex (another service to run)
- Event schema tied to database schema
- Harder to customize event payloads

**Rejected because:**
- Educational project: Want to understand pattern deeply
- Outbox pattern more common in Big Tech interviews
- More control over event format

## Alternatives Considered

### Alternative 1: No Outbox (Naive Publish)

**Code:**
```csharp
await _context.SaveChangesAsync();
await _kafka.ProduceAsync("ticket.events", event);  // Dual write!
```

**Pros:**
- Simplest code
- Immediate event publishing (no latency)

**Cons:**
- Dual write problem (data loss or phantom events)
- No guarantee of delivery

**Rejected because:**
- Violates correctness (events can be lost)
- Not production-ready pattern

### Alternative 2: Publish First, Then Save

**Code:**
```csharp
await _kafka.ProduceAsync("ticket.events", event);
await _context.SaveChangesAsync();
```

**Pros:**
- Kafka write fails fast (no wasted database work)

**Cons:**
- Still dual write (same problem)
- Worse: If Kafka succeeds but DB fails, event for non-existent ticket

**Rejected because:**
- Doesn't solve dual write
- Phantom events worse than lost events

### Alternative 3: Two-Phase Commit (2PC)

**How it works:**
- Coordinator (transaction manager) coordinates Kafka + PostgreSQL
- Prepare phase: Both systems vote
- Commit phase: Both commit or rollback

**Pros:**
- True ACID across systems

**Cons:**
- Requires XA transaction support (Kafka doesn't support)
- Blocking protocol (coordinator failure blocks everyone)
- High latency

**Rejected because:**
- Kafka doesn't support 2PC
- Outbox pattern simpler and non-blocking

### Alternative 4: Saga Pattern (Compensating Transactions)

**How it works:**
- Save ticket → Publish event → If publish fails, delete ticket

**Pros:**
- No outbox table needed

**Cons:**
- Compensations can fail (now what?)
- More complex error handling
- Requires idempotency for compensations

**Rejected because:**
- Outbox is simpler for this use case
- Saga better for long-running workflows (multiple services)

## Testing

**Unit Test:**
```csharp
[Fact]
public async Task CreateTicket_ShouldWriteToOutbox()
{
    // Arrange
    var context = CreateInMemoryContext();
    var service = new TicketService(context, null);

    // Act
    var ticket = await service.CreateTicketAsync(new CreateTicketCommand
    {
        TenantId = "tenant-1",
        Subject = "Test ticket"
    });

    // Assert
    var outboxEvent = await context.OutboxEvents.SingleOrDefaultAsync();
    outboxEvent.Should().NotBeNull();
    outboxEvent.AggregateId.Should().Be(ticket.Id);
    outboxEvent.EventType.Should().Be("ticket.created");
    outboxEvent.ProcessedAt.Should().BeNull();
}
```

**Integration Test:**
```csharp
[Fact]
public async Task OutboxPublisher_ShouldPublishToKafka()
{
    // Arrange
    var context = CreateRealContext();
    var kafkaProducer = new MockKafkaProducer();
    var publisher = new OutboxPublisher(context, kafkaProducer);

    // Create outbox event
    context.OutboxEvents.Add(new OutboxEvent
    {
        EventType = "ticket.created",
        Payload = "{\"ticketId\":\"123\"}"
    });
    await context.SaveChangesAsync();

    // Act
    await publisher.ProcessOutboxEventsAsync(CancellationToken.None);

    // Assert
    kafkaProducer.PublishedEvents.Should().ContainSingle();
    var evt = await context.OutboxEvents.SingleAsync();
    evt.ProcessedAt.Should().NotBeNull();
}
```

## Related Decisions

- ADR-003: Kafka for events (Outbox publishes to Kafka)
- ADR-005: ClickHouse for analytics (consumes events from Kafka)

## References

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [Chris Richardson on Outbox](https://www.youtube.com/watch?v=YPbGW3Fnmbc)
- [Dual Write Problem Explained](https://thorben-janssen.com/dual-writes/)

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
