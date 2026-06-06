using MessagePack;
namespace ECommerce.Olep.Schema
{
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
