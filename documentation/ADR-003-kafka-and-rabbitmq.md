# ADR-003: Kafka for Events, RabbitMQ for Tasks

## Status
Accepted

## Context

The system has two types of asynchronous operations:

**Type 1: Events** (things that happened):
- `ticket.created` - audit log, triggers routing, feeds analytics
- `message.sent` - chat history, notifications
- `ticket.status_changed` - audit trail, SLA tracking

**Type 2: Tasks** (things that need to be done):
- Send email notification
- Create Jira ticket
- Send message to Telegram channel
- Generate PDF report

We need message brokers for these async operations. Options include:
- Kafka only
- RabbitMQ only
- Both (each for different use case)

## Decision

We will use **both**:

**Kafka** for events (Domain Events, Integration Events):
- Topics: `ticket.events`, `message.events`, `audit.events`
- Consumers: Analytics Service (ClickHouse), Routing Service, SLA Service
- Retention: 7 days (for replay and debugging)
- Key: `tenant_id:aggregate_id` (for ordering)

**RabbitMQ** for tasks (Commands, Jobs):
- Queues: `email-notifications`, `jira-integration`, `telegram-bot`
- Consumers: Integration Service workers
- Retry: 3 attempts with exponential backoff
- DLQ (Dead Letter Queue): Failed tasks for manual inspection

## Consequences

### Positive

**Kafka Benefits:**
- **Durable event log**: Can replay events for debugging or rebuilding analytics
- **Multiple consumers**: ClickHouse, SLA Service, Routing Service all read same events
- **High throughput**: Millions of events/sec (far exceeds our 100K/day)
- **Ordered delivery**: Events per ticket processed in order (partition by ticket_id)

**RabbitMQ Benefits:**
- **Task semantics**: Natural fit for "do this job"
- **Retry + DLQ**: Built-in retry with exponential backoff
- **Priority queues**: Urgent tasks can jump ahead
- **Simpler for workers**: Easier than Kafka for task processing

**Separation of Concerns:**
- Events = immutable history (Kafka)
- Tasks = mutable state (RabbitMQ: pending → processing → done/failed)

### Negative

**Operational Complexity:**
- Two message brokers to run and monitor
- More infrastructure in Docker Compose
- Need to understand two different systems

**Resource Usage:**
- Kafka: ~512MB RAM minimum
- RabbitMQ: ~256MB RAM minimum
- Total: ~1GB just for messaging

### Risks

**Confusion:**
- Developers might not know which to use

**Mitigation:**
- Clear documentation: "Use Kafka if multiple consumers need it, RabbitMQ if single worker"
- Skill document with decision tree

**Kafka Overkill:**
- For 100K events/day, RabbitMQ could handle both

**Mitigation:**
- Accept as educational trade-off (Kafka is standard in Big Tech)
- Experience with Kafka valuable for interviews

## Alternatives Considered

### Alternative 1: Kafka Only

**Pros:**
- Single message broker
- Kafka can do tasks via consumer groups
- Unified mental model

**Cons:**
- Task retry is manual (consume → process → commit, or publish to retry topic)
- DLQ requires custom implementation
- Overkill for simple task queues (email sending)

**Example of complexity:**
```csharp
// Manual retry in Kafka
var msg = consumer.Consume();
try {
    await SendEmail(msg.Value);
    consumer.Commit(msg);
} catch {
    if (msg.Headers["retry-count"] < 3) {
        await producer.ProduceAsync("email-retry-topic", msg);
    } else {
        await producer.ProduceAsync("email-dlq-topic", msg);
    }
}
```

**Rejected because:** RabbitMQ handles retry/DLQ natively, reducing boilerplate.

### Alternative 2: RabbitMQ Only

**Pros:**
- Single message broker
- Simpler operations
- Can use exchanges for fanout (multiple consumers)

**Cons:**
- Not designed for event sourcing (no durable log)
- Can't replay messages after acknowledgment
- Harder to feed ClickHouse (no native Kafka Engine integration)

**Rejected because:** 
- ClickHouse has Kafka Engine (easy integration)
- Need event replay for debugging/rebuilding analytics
- Kafka is industry standard for event streaming

### Alternative 3: No Message Brokers (HTTP callbacks)

**Pros:**
- Simpler infrastructure
- Direct service-to-service calls

**Cons:**
- Tight coupling (caller must know all consumers)
- No retry/DLQ (must implement per endpoint)
- No ability to add consumers without changing producer

**Rejected because:** 
- Violates loose coupling principle
- Doesn't scale (what if we add 5 more consumers?)

## Usage Guidelines

### When to Use Kafka

**Rule:** Use Kafka if the message represents **something that happened** (past tense) and:
- Multiple services need to react (analytics, audit, notifications)
- You might need to replay events later
- Order matters within a partition (e.g., ticket lifecycle events)

**Examples:**
```csharp
// Ticket Service publishes
await _kafka.ProduceAsync("ticket.events", new TicketCreatedEvent {
    TicketId = ticket.Id,
    TenantId = ticket.TenantId,
    CreatedAt = DateTime.UtcNow,
    Subject = ticket.Subject
});

// Multiple consumers:
// - Analytics Service → writes to ClickHouse
// - Routing Service → triggers AI routing
// - SLA Service → starts SLA timer
```

### When to Use RabbitMQ

**Rule:** Use RabbitMQ if the message represents **work to be done** (future tense) and:
- Single consumer (or competing consumers for same job)
- Need retry with backoff
- Task can fail and needs DLQ

**Examples:**
```csharp
// Ticket Service publishes task
await _rabbitMQ.PublishAsync("email-notifications", new SendEmailTask {
    To = customer.Email,
    Subject = "Your ticket #123 was created",
    Body = "..."
});

// Integration Service consumer:
// - Processes task
// - Auto-retry 3x on failure
// - Moves to DLQ if still fails
```

## Decision Tree

```
Is this a message about something that already happened?
├─ Yes → Is order important? OR Do multiple services need it?
│  ├─ Yes → KAFKA (event)
│  └─ No → Could still use Kafka (for replay/audit), or RabbitMQ
└─ No → Is this work that needs to be done?
   └─ Yes → RABBITMQ (task)
```

## Implementation Notes

**Kafka Topics:**
```bash
ticket.events        # All ticket lifecycle events
message.events       # Chat messages
audit.events         # System-wide audit log
```

**RabbitMQ Queues:**
```bash
email-notifications  # Send emails
jira-integration     # Create/update Jira issues
telegram-bot         # Send Telegram messages
slack-bot            # Send Slack messages
report-generation    # Generate PDF/Excel reports
```

**Kafka Producer (Outbox Pattern):**
```csharp
public class OutboxPublisher : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var events = await _db.OutboxEvents
                .Where(e => e.ProcessedAt == null)
                .OrderBy(e => e.CreatedAt)
                .Take(100)
                .ToListAsync(ct);
            
            foreach (var evt in events)
            {
                await _kafkaProducer.ProduceAsync(
                    evt.Topic, 
                    evt.Key, 
                    evt.Payload
                );
                evt.ProcessedAt = DateTime.UtcNow;
            }
            
            await _db.SaveChangesAsync(ct);
            await Task.Delay(TimeSpan.FromSeconds(5), ct);
        }
    }
}
```

**RabbitMQ Consumer (with retry):**
```csharp
var channel = connection.CreateModel();

channel.QueueDeclare(
    queue: "email-notifications",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object> {
        { "x-dead-letter-exchange", "dlq-exchange" },
        { "x-message-ttl", 60000 } // 1 minute retry delay
    }
);

channel.BasicConsume(
    queue: "email-notifications",
    autoAck: false,
    consumer: new EventingBasicConsumer(channel)
);

consumer.Received += async (model, ea) =>
{
    try
    {
        var task = JsonSerializer.Deserialize<SendEmailTask>(ea.Body.ToArray());
        await _emailService.SendAsync(task);
        channel.BasicAck(ea.DeliveryTag, false);
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Failed to process email task");
        // Nack with requeue (will go to DLQ after max retries)
        channel.BasicNack(ea.DeliveryTag, false, requeue: false);
    }
};
```

## Related Decisions

- ADR-001: Microservices (requires async communication)
- ADR-005: ClickHouse for analytics (consumes from Kafka)
- ADR-007: Outbox Pattern (ensures Kafka delivery)

## References

- [Kafka vs RabbitMQ: When to Use Each](https://medium.com/@madhukaudantha/kafka-vs-rabbitmq-1-b1c5e574bcd)
- [Event-Driven Architecture](https://martinfowler.com/articles/201701-event-driven.html)
- [RabbitMQ DLQ Documentation](https://www.rabbitmq.com/dlx.html)

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
