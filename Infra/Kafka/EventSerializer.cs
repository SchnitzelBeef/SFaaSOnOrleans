using Confluent.Kafka;
using MessagePack;

namespace Infra.Kafka;

public class EventSerializer<TEvent> : ISerializer<TEvent>, IDeserializer<TEvent>
    where TEvent : class
{
    private readonly MessagePackSerializerOptions options = MessagePackSerializerOptions.Standard
        .WithResolver(MessagePack.Resolvers.TypelessContractlessStandardResolver.Instance);

    public byte[] Serialize(TEvent e, SerializationContext _)
    {
        var data = MessagePackSerializer.Serialize(e, options);
        return data;
    }

    public TEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
    {
        if (isNull) return null;
        var e = MessagePackSerializer.Deserialize<TEvent>(data.ToArray(), options);
        return e;
    }
}
