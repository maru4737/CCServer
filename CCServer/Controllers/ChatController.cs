using CCServer.Kafka;
using CCServer.Models;
using CCServer.Services;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace CCServer.Controllers;

[ApiController]
[Route("api/chat")]
public sealed class ChatController : ControllerBase
{
    private readonly IChatProducer _producer;
    private readonly RoomMessageStore _store;

    public ChatController(IChatProducer producer, RoomMessageStore store)
    {
        _producer = producer;
        _store = store;
    }

    public sealed record SendRequest(string RoomId, string User, string Text, string SenderId);

    [HttpPost("send")]
    public async Task<IActionResult> Send([FromBody] SendRequest req, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(req.RoomId)) return BadRequest("RoomId required");
        if (string.IsNullOrWhiteSpace(req.User)) return BadRequest("User required");
        if (string.IsNullOrWhiteSpace(req.Text)) return BadRequest("Text required");
        if (string.IsNullOrWhiteSpace(req.SenderId)) return BadRequest("SenderId required");

        var payload = new
        {
            RoomId = req.RoomId.Trim(),
            User = req.User.Trim(),
            Text = req.Text,
            Time = DateTimeOffset.UtcNow,
            SenderId = req.SenderId.Trim()
        };

        var json = JsonSerializer.Serialize(payload);

        await _producer.PublishAsync(req.RoomId, json, ct);
        return Ok();
    }

    // REST로 backlog 조회 (WS 재연결 전에 사용 가능)
    [HttpGet("history")]
    public IActionResult History(
        [FromQuery] string RoomId,
        [FromQuery] long afterSeq = 0,
        [FromQuery] int limit = 200)
    {
        if (string.IsNullOrWhiteSpace(RoomId)) return BadRequest("RoomId required");
        limit = Math.Clamp(limit, 1, 500);

        var items = _store.GetAfter(RoomId.Trim(), afterSeq, limit);
        return Ok(items);
    }
}