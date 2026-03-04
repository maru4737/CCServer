using Confluent.Kafka;

namespace CCServer.Kafka;

public interface IChatProducer
{
    Task PublishAsync(string roomId, string payloadJson, CancellationToken ct);
}

public sealed class KafkaProducer : IChatProducer, IDisposable
{
    private readonly KafkaOptions _opt;
    private readonly IProducer<string, string> _producer;

    // ✅ IOptions<KafkaOptions>가 아니라 KafkaOptions를 받도록 변경
    public KafkaProducer(KafkaOptions opt)
    {
        _opt = opt;

        if (string.IsNullOrWhiteSpace(_opt.BootstrapServers))
            throw new ArgumentException("Kafka:BootstrapServers is required");

        var config = new ProducerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public Task PublishAsync(string roomId, string payloadJson, CancellationToken ct)
    {
        return _producer.ProduceAsync(
            _opt.Topic,
            new Message<string, string> { Key = roomId, Value = payloadJson },
            ct
        );
    }

    public void Dispose()
    {
        try { _producer.Flush(TimeSpan.FromSeconds(2)); } catch { }
        _producer.Dispose();
    }
}