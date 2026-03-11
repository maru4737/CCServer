using System.Collections.Concurrent;

namespace CCServer.Services;

public sealed class RoomHub
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, WebSocketConnection>> _roomMembers = new();

    public void Join(WebSocketConnection conn, string RoomId)
    {
        var members = _roomMembers.GetOrAdd(RoomId, _ => new ConcurrentDictionary<Guid, WebSocketConnection>());
        members[conn.Id] = conn;
    }

    public void Leave(WebSocketConnection conn)
    {
        var RoomId = conn.RoomId;
        if (string.IsNullOrWhiteSpace(RoomId)) return;

        if (_roomMembers.TryGetValue(RoomId, out var members))
        {
            _ = members.TryRemove(conn.Id, out _);
            if (members.IsEmpty)
                _ = _roomMembers.TryRemove(RoomId, out _);
        }
    }

    public int BroadcastToRoom(string RoomId, object envelope)
    {
        if (!_roomMembers.TryGetValue(RoomId, out var members)) return 0;

        var ok = 0;
        foreach (var kv in members)
        {
            if (kv.Value.TryEnqueue(envelope))
                ok++;
        }
        return ok;
    }

    public int GetRoomMemberCount(string RoomId)
    {
        return _roomMembers.TryGetValue(RoomId, out var members) ? members.Count : 0;
    }

    
}