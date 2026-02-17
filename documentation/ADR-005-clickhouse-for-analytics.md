# ADR-005: ClickHouse for Analytics (OLAP)

## Status
Accepted

## Context

The system requires analytics capabilities:

**Real-time operational queries** (answered by PostgreSQL):
- "Show me ticket #12345 with full history"
- "List all open tickets assigned to agent John"
- "Get customer profile for user@example.com"

**Analytical queries** (need specialized solution):
- "What's the average first response time across all departments for the last 3 months?"
- "Show p95 and p99 resolution times by priority, grouped by week"
- "Which agents have >10% SLA breach rate?"
- "Compare department performance month-over-month for the last year"

These analytical queries:
- Scan millions of rows (historical data)
- Perform heavy aggregations (AVG, percentiles, GROUP BY)
- Don't need real-time consistency (1 minute lag is fine)

Running these on the same PostgreSQL database creates problems:
- **Resource contention**: Analytical queries compete with OLTP writes
- **Performance degradation**: Full table scans slow down transactional queries
- **Suboptimal storage**: Row-based storage inefficient for column aggregations

We need to decide: Enhance PostgreSQL or add dedicated OLAP database?

## Decision

We will use **ClickHouse** as a separate OLAP database for analytics.

**Data flow:**
```
Ticket Service → PostgreSQL (OLTP)
     ↓
  Outbox → Kafka (ticket.events)
     ↓
ClickHouse Kafka Engine (auto-consume)
     ↓
Materialized Views (transform + aggregate)
     ↓
Analytics Tables (pre-computed metrics)
```

**Analytics Service** queries ClickHouse for:
- SLA dashboard (average, p95, p99 response times)
- Historical trends (tickets per week, resolution time over time)
- Performance reports (agent/department rankings)
- Export to Excel/PDF

**PostgreSQL** remains for:
- Ticket CRUD operations
- Current ticket status
- Real-time assignment
- Transactional workflows

## Consequences

### Positive

**Performance:**
- **Columnar storage**: 100x faster for aggregations (only reads needed columns)
- **Compression**: ~10x better than row-based (repetitive data compresses well)
- **Parallel processing**: Queries utilize all CPU cores
- **Pre-aggregation**: Materialized views compute metrics on write

**Example speedup:**
```sql
-- PostgreSQL: 15 seconds (scans 10M rows)
SELECT department_id, AVG(resolution_time_seconds)
FROM ticket_events
WHERE created_at > NOW() - INTERVAL '90 days'
GROUP BY department_id;

-- ClickHouse: 50ms (reads pre-aggregated hourly metrics)
SELECT department_id, sum(total_resolution_time) / sum(tickets_resolved)
FROM sla_metrics_hourly
WHERE date >= today() - INTERVAL 90 DAY
GROUP BY department_id;
```

**Scalability:**
- OLAP queries don't affect OLTP performance
- Can scale reads independently (add ClickHouse replicas)
- TTL auto-deletes old data (e.g., keep last 2 years)

**ClickHouse-Specific Features:**
- **Kafka Engine**: Native integration (no custom consumer needed)
- **Materialized Views**: Auto-transform events on insert
- **Distributed Joins**: Can join across shards (future)

**Learning Value:**
- Experience with OLAP systems (common in Big Tech)
- Understanding of CQRS pattern (separate read/write models)
- System Design interview topic (OLTP vs OLAP trade-offs)

### Negative

**Operational Complexity:**
- Additional database to run and monitor
- 1GB RAM minimum for ClickHouse
- Need to maintain two schemas (PostgreSQL + ClickHouse)

**Eventual Consistency:**
- Analytics lag behind reality by ~1 minute
- Reports may not show very recent tickets
- Requires clear communication to users ("data refreshed every minute")

**Development Overhead:**
- Two data models to maintain
- Schema changes require updating both databases
- Debugging harder (data flows through Kafka)

**No Updates/Deletes:**
- ClickHouse is append-only (can't UPDATE/DELETE rows easily)
- Must use ReplacingMergeTree for deduplication
- Not suitable for transactional data

### Risks

**Data Sync Issues:**
- If Kafka consumer lags, analytics will be stale

**Mitigation:**
- Monitor Kafka consumer lag (alert if > 1000 messages)
- Consumer group allows horizontal scaling

**Duplicate Events:**
- Kafka at-least-once delivery may cause duplicates

**Mitigation:**
- Use ReplacingMergeTree with `event_id` deduplication key
- ClickHouse automatically removes duplicates on merge

**Storage Growth:**
- Events accumulate over time

**Mitigation:**
- Partition by month: `PARTITION BY toYYYYMM(event_time)`
- TTL: `TTL event_time + INTERVAL 2 YEAR` (auto-delete old partitions)

## Alternatives Considered

### Alternative 1: PostgreSQL Materialized Views

**Pros:**
- No additional database
- Strong consistency (data always up-to-date)
- Simpler infrastructure

**Cons:**
- Materialized views must be manually refreshed (REFRESH MATERIALIZED VIEW)
- Refresh locks table (blocks writes during refresh)
- Slow refresh for large tables (10M+ rows)
- Still row-based storage (not optimized for analytics)

**Rejected because:**
- Analytical queries would still compete with OLTP
- Materialized view refresh every minute would impact performance
- Doesn't scale (can't add read replicas for analytics only)

### Alternative 2: TimescaleDB (PostgreSQL Extension)

**Pros:**
- Stays in PostgreSQL ecosystem
- Familiar SQL
- Automatic time-series partitioning

**Cons:**
- Still row-based storage (slower than columnar)
- Less efficient for wide aggregations
- Limited to single machine (harder to scale horizontally)

**Rejected because:**
- TimescaleDB optimized for time-series inserts, not complex aggregations
- ClickHouse better for our use case (GROUP BY, percentiles, joins)

### Alternative 3: Apache Druid

**Pros:**
- Real-time analytics (lower latency than ClickHouse)
- Good for dashboards with sub-second queries

**Cons:**
- More complex setup (requires ZooKeeper, deep storage)
- Overkill for 100K events/day
- Steeper learning curve

**Rejected because:**
- ClickHouse simpler for educational project
- Don't need real-time (1 minute lag is fine)

### Alternative 4: No Separate OLAP (Keep All in PostgreSQL)

**Pros:**
- Simplest solution
- Single source of truth

**Cons:**
- Analytics queries slow down ticket creation
- Can't leverage columnar storage benefits
- Misses learning opportunity (OLAP systems)

**Rejected because:**
- Primary goal is System Design interview prep
- Need experience with OLTP/OLAP separation
- Standard pattern in Big Tech companies

## Implementation Details

### ClickHouse Schema

**Kafka Engine Table (Auto-Consumer):**
```sql
CREATE TABLE ticket_events_queue (
    event_json String
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'ticket.events',
    kafka_group_name = 'clickhouse_analytics',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3;
```

**Target Table (Stores Events):**
```sql
CREATE TABLE ticket_events (
    tenant_id String,
    ticket_id String,
    event_type String,
    event_time DateTime64(3),
    event_id String,  -- for deduplication
    agent_id String,
    department_id String,
    priority Enum8('low'=1, 'medium'=2, 'high'=3, 'urgent'=4)
) ENGINE = ReplacingMergeTree(event_time, event_id)
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, ticket_id, event_type, event_time)
TTL event_time + INTERVAL 2 YEAR;
```

**Materialized View (Transforms JSON):**
```sql
CREATE MATERIALIZED VIEW ticket_events_mv TO ticket_events AS
SELECT
    JSONExtractString(event_json, 'tenant_id') as tenant_id,
    JSONExtractString(event_json, 'ticket_id') as ticket_id,
    JSONExtractString(event_json, 'type') as event_type,
    parseDateTimeBestEffort(JSONExtractString(event_json, 'timestamp')) as event_time,
    JSONExtractString(event_json, 'event_id') as event_id,
    JSONExtractString(event_json, 'agent_id') as agent_id,
    JSONExtractString(event_json, 'department_id') as department_id,
    JSONExtractString(event_json, 'priority') as priority
FROM ticket_events_queue;
```

**Pre-Aggregated Metrics (Hourly):**
```sql
CREATE TABLE sla_metrics_hourly (
    tenant_id String,
    department_id String,
    date Date,
    hour UInt8,
    priority Enum8('low'=1, 'medium'=2, 'high'=3, 'urgent'=4),
    
    tickets_created UInt64,
    tickets_resolved UInt64,
    sla_breached UInt64,
    
    total_first_response_time UInt64,
    first_response_times Array(UInt32)  -- for percentiles
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tenant_id, department_id, date, hour, priority);

CREATE MATERIALIZED VIEW sla_metrics_hourly_mv TO sla_metrics_hourly AS
SELECT
    tenant_id,
    department_id,
    toDate(event_time) as date,
    toHour(event_time) as hour,
    priority,
    countIf(event_type = 'ticket.created') as tickets_created,
    countIf(event_type = 'ticket.resolved') as tickets_resolved,
    groupArrayIf(
        dateDiff('second', created_at, event_time),
        event_type = 'ticket.first_response'
    ) as first_response_times
FROM ticket_events
GROUP BY tenant_id, department_id, date, hour, priority, ticket_id;
```

### Analytics Service Query

```csharp
public async Task<SlaMetrics> GetDepartmentMetricsAsync(
    string tenantId, 
    DateOnly startDate, 
    DateOnly endDate)
{
    var query = @"
        SELECT
            department_id,
            sum(tickets_created) as total_tickets,
            sum(tickets_resolved) as resolved_tickets,
            
            -- Average first response time
            sum(total_first_response_time) / sum(tickets_resolved) as avg_seconds,
            
            -- Percentiles (p50, p95, p99)
            quantile(0.50)(arrayJoin(first_response_times)) as p50,
            quantile(0.95)(arrayJoin(first_response_times)) as p95,
            quantile(0.99)(arrayJoin(first_response_times)) as p99
        FROM sla_metrics_hourly
        WHERE tenant_id = {tenantId:String}
          AND date BETWEEN {startDate:Date} AND {endDate:Date}
        GROUP BY department_id";
    
    await using var connection = new ClickHouseConnection(_connectionString);
    await connection.OpenAsync();
    
    var command = connection.CreateCommand();
    command.CommandText = query;
    command.Parameters.AddWithValue("tenantId", tenantId);
    command.Parameters.AddWithValue("startDate", startDate);
    command.Parameters.AddWithValue("endDate", endDate);
    
    // Execute and map results...
}
```

## Guidelines: When to Use PostgreSQL vs ClickHouse

### Use PostgreSQL (OLTP) for:

- ✅ Create, update, delete operations
- ✅ Get single ticket by ID
- ✅ List tickets for agent (with filters, pagination)
- ✅ Current state queries ("is ticket assigned?", "what's the status?")
- ✅ Transactional workflows (change status + log event in same transaction)

### Use ClickHouse (OLAP) for:

- ✅ Aggregations over large time ranges (last 3 months, year-over-year)
- ✅ Percentile calculations (p50, p95, p99 response times)
- ✅ GROUP BY queries with millions of rows
- ✅ Historical trends (tickets per day, resolution time over time)
- ✅ Department/agent performance comparisons
- ✅ Report generation (export to Excel/PDF)

### Decision Tree:

```
Does query scan >10K rows?
├─ Yes → Is it an aggregation (COUNT, AVG, percentiles)?
│  ├─ Yes → ClickHouse
│  └─ No → PostgreSQL (with pagination)
└─ No → PostgreSQL
```

## Monitoring and Maintenance

**Metrics to Monitor:**
- Kafka consumer lag (ClickHouse consumer group)
- ClickHouse query latency (should be <1s for dashboards)
- Storage growth rate (partition sizes)
- Merge operations (ClickHouse background merges)

**Operational Tasks:**
- Weekly: Check consumer lag
- Monthly: Review partition sizes
- Quarterly: Optimize materialized views (add missing indexes)

## Related Decisions

- ADR-001: Microservices (separate Analytics Service)
- ADR-003: Kafka for events (feeds ClickHouse)
- ADR-007: Outbox Pattern (ensures data reaches ClickHouse)

## References

- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Kafka Engine in ClickHouse](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [CQRS Pattern](https://martinfowler.com/bliki/CQRS.html)
- [When to Use ClickHouse vs PostgreSQL](https://clickhouse.com/blog/clickhouse-vs-postgresql)

---

**Approved by:** [Your Name]  
**Date:** February 12, 2026
