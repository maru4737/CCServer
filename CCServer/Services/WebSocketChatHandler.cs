using CCServer.Kafka;
using CCServer.Models;
using CCServer.Utils;
using Microsoft.Extensions.Logging;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace CCServer.Services;

public sealed class WebSocketChatHandler
{
    private readonly RoomHub _hub;
    private readonly RoomMessageStore _store;
    private readonly IChatProducer _producer;
    private readonly ILogger<WebSocketChatHandler> _log;
    private readonly ILogger<WebSocketConnection> _connLog;

    public WebSocketChatHandler(
        RoomHub hub,
        RoomMessageStore store,
        IChatProducer producer,
        ILogger<WebSocketChatHandler> log,
        ILogger<WebSocketConnection> connLog)
    {
        _hub = hub;
        _store = store;
        _producer = producer;
        _log = log;
        _connLog = connLog;
    }

    public async Task HandleAsync(WebSocket socket, CancellationToken ct)
    {
        // ✅ ServiceProvider 쓰지 말고 ILogger<WebSocketConnection> 직접 주입받아서 사용
        var conn = new WebSocketConnection(socket, _connLog);

        // SendLoop는 반드시 돌려야 실제 전송이 됩니다.
        var sendLoop = Task.Run(() => conn.SendLoopAsync(ct), ct);

        _log.LogInformation("WS connected. ConnId={ConnId} State={State}", conn.Id, socket.State);

        try
        {
            var joined = await ReceiveJoinAsync(conn, ct);
            if (!joined) return;

            await ReceiveLoopAsync(conn, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.LogError(ex, "WS handler error. ConnId={ConnId}", conn.Id);
        }
        finally
        {
            // 방/연결 정리
            _hub.Leave(conn);
            conn.CompleteSendQueue();

            _log.LogInformation("WS disconnected. ConnId={ConnId} Room={RoomId} User={User} State={State}",
                conn.Id, conn.RoomId, conn.User, socket.State);

            try
            {
                if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
            }
            catch { /* ignore */ }

            try { await sendLoop; } catch { /* ignore */ }
        }
    }

    private async Task<bool> ReceiveJoinAsync(WebSocketConnection conn, CancellationToken ct)
    {
        var env = await ReceiveEnvelopeAsync(conn.Socket, ct);
        if (env is null)
        {
            conn.TryEnqueue(new WsServerEnvelope { Type = "error", Payload = "join required" });
            return false;
        }

        if (!string.Equals(env.Type, "join", StringComparison.OrdinalIgnoreCase))
        {
            conn.TryEnqueue(new WsServerEnvelope { Type = "error", Payload = "first message must be join" });
            return false;
        }

        var roomId = env.RoomId?.Trim();
        var user = env.User?.Trim();
        var senderId = env.SenderId?.Trim();
        var afterSeq = env.AfterSeq ?? 0;

        if (string.IsNullOrWhiteSpace(roomId) ||
            string.IsNullOrWhiteSpace(user) ||
            string.IsNullOrWhiteSpace(senderId))
        {
            conn.TryEnqueue(new WsServerEnvelope { Type = "error", Payload = "roomId/user/senderId required" });
            return false;
        }

        conn.BindIdentity(roomId, user, senderId);
        _hub.Join(conn, roomId);

        _log.LogInformation("JOIN ok. ConnId={ConnId} Room={RoomId} User={User} AfterSeq={AfterSeq}",
            conn.Id, roomId, user, afterSeq);

        // ✅ payload도 camelCase로 통일(클라 혼선 방지)
        conn.TryEnqueue(new WsServerEnvelope
        {
            Type = "joined",
            RoomId = roomId,
            Payload = new { roomId, user }
        });

        var backlog = _store.GetAfter(roomId, afterSeq, limit: 200);
        _log.LogInformation("Backlog count={Count} Room={RoomId} AfterSeq={AfterSeq}", backlog.Count, roomId, afterSeq);

        if (backlog.Count > 0)
        {
            conn.TryEnqueue(new WsServerEnvelope
            {
                Type = "backlog",
                RoomId = roomId,
                Payload = backlog
            });
        }

        return true;
    }

    private async Task ReceiveLoopAsync(WebSocketConnection conn, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && conn.Socket.State == WebSocketState.Open)
        {
            var env = await ReceiveEnvelopeAsync(conn.Socket, ct);
            if (env is null) break;

            if (string.Equals(env.Type, "ping", StringComparison.OrdinalIgnoreCase))
            {
                conn.TryEnqueue(new WsServerEnvelope { Type = "pong" });
                continue;
            }

            if (string.Equals(env.Type, "send", StringComparison.OrdinalIgnoreCase))
            {
                var text = env.Text?.Trim();
                if (string.IsNullOrWhiteSpace(text)) continue;

                _log.LogInformation("WS SEND received. ConnId={ConnId} Room={RoomId} User={User} TextLen={Len}",
                    conn.Id, conn.RoomId, conn.User, text.Length);

                // ✅ Kafka로 보낼 JSON도 camelCase 통일
                var payloadJson = JsonSerializer.Serialize(new
                {
                    roomId = conn.RoomId!,
                    user = conn.User!,
                    text,
                    time = DateTimeOffset.UtcNow,
                    senderId = conn.SenderId!
                }, JsonDefaults.Web);

                await _producer.PublishAsync(conn.RoomId!, payloadJson, ct);

                _log.LogInformation("Kafka produced (awaited). Room={RoomId} User={User}", conn.RoomId, conn.User);
                continue;
            }

            conn.TryEnqueue(new WsServerEnvelope { Type = "error", Payload = $"unknown type: {env.Type}" });
        }
    }

    private static async Task<WsClientEnvelope?> ReceiveEnvelopeAsync(WebSocket socket, CancellationToken ct)
    {
        var buffer = new byte[8 * 1024];
        using var ms = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close) return null;

            ms.Write(buffer, 0, result.Count);

            if (result.EndOfMessage) break;

            // 너무 큰 메시지 방어
            if (ms.Length > 256 * 1024) return null;
        }

        var json = Encoding.UTF8.GetString(ms.ToArray());

        try
        {
            // 클라 → 서버는 Pascal/camel 섞여도 일단 받게(필요하면 옵션 추가 가능)
            return JsonSerializer.Deserialize<WsClientEnvelope>(json);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}