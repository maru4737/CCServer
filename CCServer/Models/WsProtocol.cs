using System.Text.Json.Serialization;

namespace CCServer.Models;

public sealed class WsClientEnvelope
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = default!; // "join" | "send" | "ping"

    [JsonPropertyName("RoomId")]
    public string? RoomId { get; init; }

    [JsonPropertyName("user")]
    public string? User { get; init; }

    [JsonPropertyName("text")]
    public string? Text { get; init; }

    [JsonPropertyName("senderId")]
    public string? SenderId { get; init; }

    [JsonPropertyName("afterSeq")]
    public long? AfterSeq { get; init; }
}

public sealed class WsServerEnvelope
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = default!; // "joined" | "message" | "pong" | "error" | "backlog"

    [JsonPropertyName("RoomId")]
    public string? RoomId { get; init; }

    [JsonPropertyName("payload")]
    public object? Payload { get; init; }
}