using Confluent.Kafka;
using ManualWork.Options;
using Microsoft.Extensions.Options;

namespace ManualWork.Services;

public class KafkaProducerService
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly IProducer<string, string> _producer;

    public KafkaProducerService(
        ILogger<KafkaProducerService> logger,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task SendMessageAsync(string topic, string key, string value)
    {
        try
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value
            };

            var deliveryResult = await _producer.ProduceAsync(topic, message);
            
            _logger.LogInformation(
                "Message delivered to topic {Topic}, partition {Partition}, offset {Offset}",
                deliveryResult.Topic,
                deliveryResult.Partition.Value,
                deliveryResult.Offset.Value);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Error producing message to topic {Topic}", topic);
            throw;
        }
    }
}
