using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;

namespace ManualWork.Services;

public class ClickHouseService(ClickHouseConnection connection)
{
    public async Task EnsureTableAsync()
    {
        await connection.OpenAsync();

        var cmd = connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS client (
                id          UUID            DEFAULT generateUUIDv4(),
                first_name  String,
                last_name   String,
                email       String,
                phone       String,
                country     String,
                created_at  DateTime        DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (created_at, id)
            """;

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertSampleDataAsync()
    {
        var bulk = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "client",
            ColumnNames = ["first_name", "last_name", "email", "phone", "country"],
            BatchSize = 10
        };

        var rows = Enumerable.Range(1, 10).Select(i => new object[]
        {
            $"FirstName{i}",
            $"LastName{i}",
            $"user{i}@example.com",
            $"+7900000000{i:D2}",
            i % 2 == 0 ? "UK" : "Kazakhstan"
        });

        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows);
    }

    public async Task<List<ClientRecord>> SelectAllAsync()
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT id, first_name, last_name, email, phone, country, created_at FROM client";

        var results = new List<ClientRecord>();

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.Add(new ClientRecord(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.GetDateTime(6)
            ));
        }

        return results;
    }
}

public record ClientRecord(
    string Id,
    string FirstName,
    string LastName,
    string Email,
    string Phone,
    string Country,
    DateTime CreatedAt
);
