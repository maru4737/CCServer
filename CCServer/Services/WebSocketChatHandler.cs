using CCServer.Kafka;
using CCServer.Models;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace CCServer.Services;

public sealed class WebSocketChatHandler
{
    private readonly RoomHub _hub;
    private readonly RoomMessageStore _store;
    private readonly IChatProducer _producer;

    public WebSocketChatHandler(RoomHub hub, RoomMessageStore store, IChatProducer producer)
    {
        _hub = hub;
        _store = store;
        _producer = producer;
    }

    public async Task HandleAsync(WebSocket socket, CancellationToken ct)
    {
        var conn = new WebSocketConnection(socket);
        var sendLoop = Task.Run(() => conn.SendLoopAsync(ct), ct);

        try
        {
            // 1) join 강제
            var joined = await ReceiveJoinAsync(conn, ct);
            if (!joined) return;

            // 2) 이후 수신 루프
            await ReceiveLoopAsync(conn, ct);
        }
        finally
        {
            _hub.Leave(conn);
            conn.CompleteSendQueue();

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

        conn.TryEnqueue(new WsServerEnvelope
        {
            Type = "joined",
            RoomId = roomId,
            Payload = new { roomId, user }
        });

        // backlog
        var backlog = _store.GetAfter(roomId, afterSeq, limit: 200);
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
                var roomId = conn.RoomId!;
                var user = conn.User!;
                var senderId = conn.SenderId!;

                var text = env.Text?.Trim();
                if (string.IsNullOrWhiteSpace(text)) continue;

                // 브로드캐스트는 하지 않고 Kafka로만 발행
                var payloadJson = JsonSerializer.Serialize(new
                {
                    RoomId = roomId,
                    User = user,
                    Text = text,
                    Time = DateTimeOffset.UtcNow,
                    SenderId = senderId
                });

                await _producer.PublishAsync(roomId, payloadJson, ct);
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

            // 너무 큰 메시지 방어(256KB)
            if (ms.Length > 256 * 1024) return null;
        }

        var json = Encoding.UTF8.GetString(ms.ToArray());
        try
        {
            return JsonSerializer.Deserialize<WsClientEnvelope>(json);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}