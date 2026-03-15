using Grpc.Core;
using GreeterServiceApp;

namespace ManualWork.Services;

public class GreeterService : Greeter.GreeterBase
{
    private readonly ILogger<GreeterService> _logger;
    private readonly KafkaProducerService _kafkaProducer;

    public GreeterService(ILogger<GreeterService> logger, KafkaProducerService kafkaProducer)
    {
        _logger = logger;
        _kafkaProducer = kafkaProducer;
    }

    public override async Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
    {
        await _kafkaProducer.SendMessageAsync("test-topic", Guid.NewGuid().ToString(), request.Name);

        return new HelloReply
        {
            Message = $"Hello, {request.Name}!"
        };
    }
}