using MessagePack;

namespace Infra.EventSchema
{
    [MessagePackObject(keyAsPropertyName: true)]
    public sealed class Inventory
    {
        public long customerId { get; set; }
        public double price { get; set; }
        public int quantity { get; set; }

        [SerializationConstructor]
        public Inventory(long customerId, double price, int quantity)
        {
            this.customerId = customerId;
            this.price = price;
            this.quantity = quantity;
        }
    }
}
