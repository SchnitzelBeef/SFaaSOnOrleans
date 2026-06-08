using MessagePack;

namespace Infra.EventSchema
{
    [MessagePackObject(keyAsPropertyName: true)]
    public class Checkout
    {
        public long productId { get; set; }
        public double price { get; set; }
        public int quantity { get; set; }

        public Checkout(long productId, double price, int quantity)
        {
            this.productId = productId;
            this.price = price;
            this.quantity = quantity;
        }
    }
}