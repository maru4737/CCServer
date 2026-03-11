using Microsoft.Extensions.Logging;
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

    private readonly ILogger<WebSocketConnection> _log;

    private readonly Channel<string> _sendQueue = Channel.CreateBounded<string>(new BoundedChannelOptions(500)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropOldest
    });

    public WebSocketConnection(WebSocket socket, ILogger<WebSocketConnection> log)
    {
        Socket = socket;
        _log = log;
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

        var ok = _sendQueue.Writer.TryWrite(json);
        if (!ok)
        {
            // 큐가 터질 때만 경고
            _log.LogWarning("Send queue full. ConnId={ConnId} Room={RoomId} User={User}", Id, RoomId, User);
        }
        return ok;
    }

    public async Task SendLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var json in _sendQueue.Reader.ReadAllAsync(ct))
            {
                if (Socket.State != WebSocketState.Open)
                    break;

                var bytes = Encoding.UTF8.GetBytes(json);
                await Socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            _log.LogWarning(ex, "WebSocket send failed. ConnId={ConnId} Room={RoomId} User={User}", Id, RoomId, User);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "SendLoop crashed. ConnId={ConnId}", Id);
        }
    }

    public void CompleteSendQueue()
        => _sendQueue.Writer.TryComplete();
}