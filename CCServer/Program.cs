using CCServer.Kafka;
using CCServer.Realtime;
using CCServer.Services;
using Microsoft.AspNetCore.WebSockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// WebSocket: Kestrel에서 WebSocket 요청 업그레이드 가능하도록 설정
builder.Services.AddWebSockets(o =>
{
    o.KeepAliveInterval = TimeSpan.FromSeconds(30);
});

// Options 바인딩
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));

// Core services
builder.Services.AddSingleton<RoomMessageStore>();
builder.Services.AddSingleton<RoomHub>();
builder.Services.AddSingleton<ChatStreamHub>(); // 남겨도 무방
builder.Services.AddSingleton<WebSocketChatHandler>();

// Kafka Producer (KafkaOptions 값을 꺼내서 생성자에 전달)
builder.Services.AddSingleton<IChatProducer>(sp =>
{
    var opt = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new KafkaProducer(opt); // ✅ KafkaProducer가 KafkaOptions 받게 맞춰야 함
});

// Kafka Consumer Worker
builder.Services.AddHostedService<KafkaConsumerWorker>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// 실제 WebSocket 미들웨어 활성화
app.UseWebSockets();

app.MapControllers();
app.Run();