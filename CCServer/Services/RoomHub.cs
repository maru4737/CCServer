using System.Collections.Concurrent;

namespace CCServer.Services;

public sealed class RoomHub
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, WebSocketConnection>> _roomMembers = new();

    public void Join(WebSocketConnection conn, string roomId)
    {
        var members = _roomMembers.GetOrAdd(roomId, _ => new ConcurrentDictionary<Guid, WebSocketConnection>());
        members[conn.Id] = conn;
    }

    public void Leave(WebSocketConnection conn)
    {
        var roomId = conn.RoomId;
        if (string.IsNullOrWhiteSpace(roomId)) return;

        if (_roomMembers.TryGetValue(roomId, out var members))
        {
            _ = members.TryRemove(conn.Id, out _);
            if (members.IsEmpty)
                _ = _roomMembers.TryRemove(roomId, out _);
        }
    }

    public void BroadcastToRoom(string roomId, object envelope)
    {
        if (!_roomMembers.TryGetValue(roomId, out var members)) return;

        foreach (var kv in members)
            _ = kv.Value.TryEnqueue(envelope);
    }
}