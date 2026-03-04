namespace CCServer.Kafka;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; init; } = default!;
    public string Topic { get; init; } = "chat.cmd.v1";
    public string ConsumerGroupId { get; init; } = "ccserver-ws-consumer";
}