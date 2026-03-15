using Confluent.Kafka;
using ManualWork.Options;
using Microsoft.Extensions.Options;

namespace ManualWork.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly KafkaOptions _kafkaOptions;

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(async () =>
        {
            IConsumer<string, string>? consumer = null;

            try
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = _kafkaOptions.BootstrapServers,
                    GroupId = _kafkaOptions.GroupId,
                    AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_kafkaOptions.AutoOffsetReset, true),
                    EnableAutoCommit = _kafkaOptions.EnableAutoCommit,
                    SessionTimeoutMs = 10000,
                    MaxPollIntervalMs = 300000
                };

                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe("test-topic");
                _logger.LogInformation("Kafka consumer started for topic: test-topic");

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (consumeResult != null && !consumeResult.IsPartitionEOF)
                        {
                            _logger.LogInformation(
                                "Received message: Key = {Key}, Value = {Value}, Partition = {Partition}, Offset = {Offset}",
                                consumeResult.Message.Key,
                                consumeResult.Message.Value,
                                consumeResult.Partition.Value,
                                consumeResult.Offset.Value);

                            await ProcessMessage(consumeResult.Message.Key, consumeResult.Message.Value);

                            if (!_kafkaOptions.EnableAutoCommit)
                            {
                                consumer.Commit(consumeResult);
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error consuming message");
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer is stopping");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in Kafka consumer. Consumer will stop.");
            }
            finally
            {
                if (consumer != null)
                {
                    try
                    {
                        consumer.Close();
                        consumer.Dispose();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error disposing Kafka consumer");
                    }
                }
            }
        }, stoppingToken);
    }

    private async Task ProcessMessage(string key, string value)
    {
        _logger.LogInformation("Processing message with key: {Key}, value: {Value}", key, value);
        await Task.CompletedTask;
    }
}
