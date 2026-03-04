namespace CCServer.Models;

public sealed record ChatMessage(
    long Seq,
    string RoomId,
    string User,
    string Text,
    DateTimeOffset Time,
    string SenderId
);