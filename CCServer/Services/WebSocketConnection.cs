using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace CCServer.Services;

public sealed class WebSocketConnection
{
    public Guid Id { get; } = Guid.NewGuid();
    public WebSocket Socket { get; }

    public string? RoomId { get; private set; }
    public string? User { get; private set; }
    public string? SenderId { get; private set; }

    // 느린 클라이언트 보호: bounded 큐 (가득 차면 오래된 메시지 드롭)
    private readonly Channel<string> _sendQueue = Channel.CreateBounded<string>(new BoundedChannelOptions(500)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropOldest
    });

    public WebSocketConnection(WebSocket socket)
    {
        Socket = socket;
    }

    public void BindIdentity(string roomId, string user, string senderId)
    {
        RoomId = roomId;
        User = user;
        SenderId = senderId;
    }

    public bool TryEnqueue(object envelope)
    {
        var json = JsonSerializer.Serialize(envelope);
        return _sendQueue.Writer.TryWrite(json);
    }

    public async Task SendLoopAsync(CancellationToken ct)
    {
        await foreach (var json in _sendQueue.Reader.ReadAllAsync(ct))
        {
            if (Socket.State != WebSocketState.Open) break;

            var bytes = Encoding.UTF8.GetBytes(json);
            await Socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, ct);
        }
    }

    public void CompleteSendQueue() => _sendQueue.Writer.TryComplete();
}