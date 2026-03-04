using CCServer.Models;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace CCServer.Realtime;

public sealed class ChatStreamHub
{
    private readonly ConcurrentDictionary<Guid, Channel<ChatMessage>> _subscribers = new();

    public (Guid id, ChannelReader<ChatMessage> reader) Subscribe()
    {
        var id = Guid.NewGuid();
        var ch = Channel.CreateUnbounded<ChatMessage>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _subscribers[id] = ch;
        return (id, ch.Reader);
    }

    public void Unsubscribe(Guid id)
    {
        if (_subscribers.TryRemove(id, out var ch))
            ch.Writer.TryComplete();
    }

    public void Publish(ChatMessage msg)
    {
        foreach (var kv in _subscribers)
        {
            _ = kv.Value.Writer.TryWrite(msg);
        }
    }
}