using CCServer.Models;
using CCServer.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace CCServer.Kafka;

public sealed class KafkaConsumerWorker : BackgroundService
{
    private readonly KafkaOptions _opt;
    private readonly RoomMessageStore _store;
    private readonly RoomHub _hub;
    private readonly ILogger<KafkaConsumerWorker> _log;

    private static readonly JsonSerializerOptions KafkaJsonOpt = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public KafkaConsumerWorker(
        IOptions<KafkaOptions> opt,
        RoomMessageStore store,
        RoomHub hub,
        ILogger<KafkaConsumerWorker> log)
    {
        _opt = opt.Value;
        _store = store;
        _hub = hub;
        _log = log;
    }

    // Producer가 넣는 JSON 스키마와 동일해야 함
    private sealed record Incoming(string RoomId, string User, string Text, DateTimeOffset Time, string SenderId);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            GroupId = _opt.ConsumerGroupId,

            // 디버깅 시 Earliest 권장(기존 메시지도 확인)
            AutoOffsetReset = AutoOffsetReset.Earliest,

            EnableAutoCommit = false
        };

        return Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) =>
                {
                    // 브로커/네트워크 경고
                    _log.LogWarning("Kafka consumer error: {Reason} IsFatal={IsFatal}", e.Reason, e.IsFatal);
                })
                .Build();

            consumer.Subscribe(_opt.Topic);

            _log.LogInformation(
                "Kafka consumer started. Topic={Topic} GroupId={GroupId} Bootstrap={Bootstrap}",
                _opt.Topic, _opt.ConsumerGroupId, _opt.BootstrapServers);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? cr;
                    try
                    {
                        cr = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _log.LogError(ex, "Kafka consume failed. Reason={Reason}", ex.Error.Reason);
                        continue;
                    }

                    if (cr?.Message?.Value is null)
                        continue;

                    _log.LogInformation(
                        "Kafka consumed. Topic={Topic} Partition={Partition} Offset={Offset} Key={Key}",
                        cr.Topic, cr.Partition.Value, cr.Offset.Value, cr.Message.Key);

                    Incoming dto;
                    try
                    {
                        dto = JsonSerializer.Deserialize<Incoming>(cr.Message.Value, KafkaJsonOpt)?? throw new JsonException("Deserialize returned null");
                    }
                    catch (Exception ex) when (ex is JsonException or NotSupportedException)
                    {
                        _log.LogWarning(ex, "Kafka message parse failed. Raw={Raw}", cr.Message.Value);

                        // 파싱 불가면 스킵(원하면 DLQ로 보내기)
                        try { consumer.Commit(cr); } catch (KafkaException kex) { _log.LogWarning(kex, "Commit failed."); }
                        continue;
                    }

                    // 필수 값 방어
                    if (string.IsNullOrWhiteSpace(dto.RoomId))
                    {
                        _log.LogWarning("Kafka message invalid (RoomId empty). Raw={Raw}", cr.Message.Value);
                        try { consumer.Commit(cr); } catch (KafkaException kex) { _log.LogWarning(kex, "Commit failed."); }
                        continue;
                    }

                    // 저장(Seq 부여)
                    ChatMessage msg;
                    try
                    {
                        msg = _store.Append(dto.RoomId, dto.User, dto.Text, dto.Time, dto.SenderId);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Store append failed. Room={RoomId}", dto.RoomId);
                        // 이 경우 커밋하지 않고 재시도하게 둘 수도 있음(현재는 문제 원인 파악이 우선)
                        continue;
                    }

                    // 브로드캐스트 (핵심: 몇 명에게 enqueue 됐는지)
                    try
                    {
                        var members = _hub.GetRoomMemberCount(dto.RoomId);

                        // ⚠️ RoomHub.BroadcastToRoom이 int 반환(Enqueued count)라고 가정
                        // 만약 아직 void라면 RoomHub를 먼저 수정해야 합니다.
                        var enqueued = _hub.BroadcastToRoom(dto.RoomId, new
                        {
                            type = "message",
                            RoomId = dto.RoomId,
                            payload = msg
                        });

                        _log.LogInformation(
                            "Broadcast result. Room={RoomId} Members={Members} Enqueued={Enqueued} Seq={Seq}",
                            dto.RoomId, members, enqueued, msg.Seq);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Broadcast failed. Room={RoomId} Seq={Seq}", dto.RoomId, msg.Seq);
                        // 브로드캐스트 실패 시 커밋 여부는 정책이지만, 지금은 원인 파악 위해 커밋 안 하고 재시도도 가능
                        // 여기서는 일단 재시도하게 커밋하지 않음
                        continue;
                    }

                    // 커밋
                    try
                    {
                        consumer.Commit(cr);
                    }
                    catch (KafkaException ex)
                    {
                        _log.LogWarning(ex, "Commit failed. Topic={Topic} Partition={Partition} Offset={Offset}",
                            cr.Topic, cr.Partition.Value, cr.Offset.Value);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // 정상 종료
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Kafka consumer crashed.");
            }
            finally
            {
                try { consumer.Close(); } catch { }
                _log.LogInformation("Kafka consumer stopped.");
            }
        }, stoppingToken);
    }
}