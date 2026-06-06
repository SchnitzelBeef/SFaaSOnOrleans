using MessagePack;
using System.Text.Json;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class ProductState : ICloneable
    {
        [Key(0)]
        public int Quantity { get; set; }
        [Key(1)]
        public double Price { get; set; }
        [Key(2)]
        public Dictionary<int, long> LastInventoryEventSequenceNumbers { get; set; }
        public object Clone()
        {
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<ProductState>(temp);
        }

        public ProductState(int quantity, double price)
        {
            this.Quantity = quantity;
            this.Price = price;
            // Currently not used:
            this.LastInventoryEventSequenceNumbers = new Dictionary<int, long>();
        }
    }
}
