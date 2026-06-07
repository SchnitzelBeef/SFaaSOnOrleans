using MessagePack;
using Confluent.Kafka;
namespace Infra.EventSchema
{

    // public class CheckoutSerializer : ISerializer<Checkout>, IDeserializer<Checkout>
    // {
    //     private readonly MessagePackSerializerOptions options = MessagePackSerializerOptions.Standard.WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);

    //     public byte[] Serialize(Checkout e, SerializationContext _)
    //     {
    //         var data = MessagePackSerializer.Serialize(e, options);
    //         return data;
    //     }

    //     public Checkout Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
    //     {
    //         if (isNull) {
    //                 Console.WriteLine("Data received is null!");
    //             return null;
    //         }
    //         var e = MessagePackSerializer.Deserialize<Checkout>(data.ToArray(), options);
    //         return e;
    //     }
    // }


    [MessagePackObject]
    public sealed class Checkout
    {
        [Key(0)]
        public readonly long productId;
        [Key(1)]
        public readonly double price;
        [Key(2)]
        public readonly int quantity;

        [SerializationConstructor]
        public Checkout(long productId, double price, int quantity)
        {
            this.productId = productId;
            this.price = price;
            this.quantity = quantity;
        }
    }
}
