using CCServer.Models;
using System.Collections.Concurrent;

namespace CCServer.Services;

public sealed class RoomMessageStore
{
    private readonly ConcurrentDictionary<string, RoomBuffer> _rooms = new();

    // 방별 backlog 보관 개수(테스트용)
    private const int DefaultCapacity = 2_000;

    public ChatMessage Append(string RoomId, string user, string text, DateTimeOffset time, string senderId)
    {
        var room = _rooms.GetOrAdd(RoomId, _ => new RoomBuffer(DefaultCapacity));
        return room.Append(RoomId, user, text, time, senderId);
    }

    public IReadOnlyList<ChatMessage> GetAfter(string RoomId, long afterSeq, int limit)
    {
        if (!_rooms.TryGetValue(RoomId, out var room))
            return Array.Empty<ChatMessage>();

        return room.GetAfter(afterSeq, limit);
    }

    private sealed class RoomBuffer
    {
        private readonly int _capacity;
        private readonly LinkedList<ChatMessage> _list = new();
        private readonly object _lock = new();
        private long _seq = 0;

        public RoomBuffer(int capacity) => _capacity = capacity;

        public ChatMessage Append(string RoomId, string user, string text, DateTimeOffset time, string senderId)
        {
            var seq = Interlocked.Increment(ref _seq);
            var msg = new ChatMessage(seq, RoomId, user, text, time, senderId);

            lock (_lock)
            {
                _list.AddLast(msg);
                while (_list.Count > _capacity)
                    _list.RemoveFirst();
            }

            return msg;
        }

        public IReadOnlyList<ChatMessage> GetAfter(long afterSeq, int limit)
        {
            lock (_lock)
            {
                var result = new List<ChatMessage>(Math.Min(limit, 256));
                foreach (var m in _list)
                {
                    if (m.Seq <= afterSeq) continue;
                    result.Add(m);
                    if (result.Count >= limit) break;
                }
                return result;
            }
        }
    }
}