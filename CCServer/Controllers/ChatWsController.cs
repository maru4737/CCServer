using CCServer.Services;
using Microsoft.AspNetCore.Mvc;

namespace CCServer.Controllers;

[ApiController]
public sealed class ChatWsController : ControllerBase
{
    private readonly WebSocketChatHandler _handler;

    public ChatWsController(WebSocketChatHandler handler)
    {
        _handler = handler;
    }

    [HttpGet("/ws")]
    public async Task Get(CancellationToken ct)
    {
        if (!HttpContext.WebSockets.IsWebSocketRequest)
        {
            HttpContext.Response.StatusCode = StatusCodes.Status400BadRequest;
            await HttpContext.Response.WriteAsync("WebSocket request required", ct);
            return;
        }

        using var socket = await HttpContext.WebSockets.AcceptWebSocketAsync();
        await _handler.HandleAsync(socket, ct);
    }
}