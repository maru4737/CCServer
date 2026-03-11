using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CCServer.Kafka;

public interface IChatProducer
{
    Task PublishAsync(string roomId, string payloadJson, CancellationToken ct);
}

public sealed class KafkaProducer : IChatProducer, IDisposable
{
    private readonly KafkaOptions _opt;
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _log;

    public KafkaProducer(KafkaOptions opt, ILogger<KafkaProducer> log)
    {
        _opt = opt;
        _log = log;

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

    public async Task PublishAsync(string RoomId, string payloadJson, CancellationToken ct)
    {
            // 호출
        _log.LogInformation("Kafka Produce start. Topic={Topic} Key(Room)={RoomId} Bytes={Bytes}",
            _opt.Topic, RoomId, payloadJson.Length);

        try
        {
            var dr = await _producer.ProduceAsync(
                _opt.Topic,
                new Message<string, string> { Key = RoomId, Value = payloadJson },
                ct
            );

                  // ack 결과 (이게 핵심)
            _log.LogInformation("Kafka Produce OK. Topic={Topic} Partition={Partition} Offset={Offset} Key={Key}",
                dr.Topic, dr.Partition.Value, dr.Offset.Value, dr.Message.Key);
        }
        catch (ProduceException<string, string> ex)
        {
                  // 브로커/전송 오류
            _log.LogError(ex, "Kafka Produce FAIL. Reason={Reason} IsFatal={IsFatal}",
                ex.Error.Reason, ex.Error.IsFatal);
            throw;
        }
        catch (OperationCanceledException)
        {
            _log.LogWarning("Kafka Produce canceled.");
            throw;
        }
    }

    public void Dispose()
    {
        try { _producer.Flush(TimeSpan.FromSeconds(2)); } catch { }
        _producer.Dispose();
    }
}