using Confluent.Kafka;
using Newtonsoft.Json;
// Obs, switched to Newtonsoft serializer since MessagePacks had issues serializing classes such as 'Inventory'

namespace Infra.Kafka;

public class EventSerializer : ISerializer<Event>, IDeserializer<Event>
{
    private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
    {
        TypeNameHandling = TypeNameHandling.All
    };

    public byte[] Serialize(Event e, SerializationContext _)
    {
        var json = JsonConvert.SerializeObject(e, Settings);
        return System.Text.Encoding.UTF8.GetBytes(json);
    }

    public Event Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
    {
        if (isNull)
        {
            Console.WriteLine("Data received is null!");
            return null;
        }
        var json = System.Text.Encoding.UTF8.GetString(data);
        return JsonConvert.DeserializeObject<Event>(json, Settings);
    }
}