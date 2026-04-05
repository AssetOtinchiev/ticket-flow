using ManualWork.Options;
using ManualWork.Services;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = builder.Configuration.GetConnectionString("Redis");
    return ConnectionMultiplexer.Connect(configuration);
});

builder.Services.AddScoped<RedisCacheService>();

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));

builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerService>();

builder.Services.AddGrpc();
builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGrpcService<GreeterService>();

app.MapGet("/api/status", async (RedisCacheService redisCacheService) =>
    {
        var value = await redisCacheService.GetAsync<string>("key");
        if (value == null)
            await redisCacheService.SetAsync("key", "key", TimeSpan.FromMinutes(5));
        
        return Results.Ok(new { status = "running", timestamp = DateTime.UtcNow });
})
.WithName("GetStatus")
.WithOpenApi();

app.Run();