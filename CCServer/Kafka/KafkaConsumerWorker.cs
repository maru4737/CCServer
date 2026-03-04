using CCServer.Models;
using CCServer.Services;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Confluent.Kafka;

namespace CCServer.Kafka;

public sealed class KafkaConsumerWorker : BackgroundService
{
    private readonly KafkaOptions _opt;
    private readonly RoomMessageStore _store;
    private readonly RoomHub _hub;

    public KafkaConsumerWorker(IOptions<KafkaOptions> opt, RoomMessageStore store, RoomHub hub)
    {
        _opt = opt.Value;
        _store = store;
        _hub = hub;
    }

    private sealed record Incoming(string RoomId, string User, string Text, DateTimeOffset Time, string SenderId);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_opt.BootstrapServers))
            throw new InvalidOperationException("Kafka:BootstrapServers is required");

        var config = new ConsumerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            GroupId = _opt.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
        };

        return Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(_opt.Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var cr = consumer.Consume(stoppingToken);
                    if (cr?.Message?.Value is null) continue;

                    Incoming? dto = null;
                    try
                    {
                        dto = JsonSerializer.Deserialize<Incoming>(cr.Message.Value);
                    }
                    catch (JsonException)
                    {
                        // TODO: 로깅 / DLQ
                    }

                    if (dto is null) continue;
                    if (string.IsNullOrWhiteSpace(dto.RoomId)) continue;

                    // 서버에서 Seq 부여 + backlog 저장
                    ChatMessage msg = _store.Append(dto.RoomId, dto.User, dto.Text, dto.Time, dto.SenderId);

                    // 브로드캐스트는 "Kafka에서 읽었을 때만"
                    _hub.BroadcastToRoom(dto.RoomId, new
                    {
                        type = "message",
                        roomId = dto.RoomId,
                        payload = msg
                    });

                    consumer.Commit(cr);
                }
            }
            catch (OperationCanceledException)
            {
                // 정상 종료
            }
            finally
            {
                try { consumer.Close(); } catch { }
            }
        }, stoppingToken);
    }
}