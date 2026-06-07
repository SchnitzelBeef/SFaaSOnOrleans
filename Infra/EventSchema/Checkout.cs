using MessagePack;

namespace Infra.EventSchema
{
    [MessagePackObject(keyAsPropertyName: true)]
    public class Checkout
    {
        public int productId { get; set; }
        public double price { get; set; }
        public int quantity { get; set; }

        public Checkout(int productId, double price, int quantity)
        {
            this.productId = productId;
            this.price = price;
            this.quantity = quantity;
        }
    }
}