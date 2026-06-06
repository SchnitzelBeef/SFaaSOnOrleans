using MessagePack;

namespace ECommerce.Olep.Schema
{
    [MessagePackObject]
    public sealed class Inventory
    {
        [Key(0)]
        public readonly long customerId;
        [Key(1)]
        public readonly double price;
        [Key(2)]
        public readonly int quantity;

        [SerializationConstructor]
        public Inventory(long customerId, double price, int quantity)
        {
            this.customerId = customerId;
            this.price = price;
            this.quantity = quantity;
        }
    }
}
