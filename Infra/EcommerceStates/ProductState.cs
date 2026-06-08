using MessagePack;
using System.Text.Json;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class ProductState
    {
        public int Quantity { get; set; }
        public double Price { get; set; }
        // public Dictionary<int, long> LastInventoryEventSequenceNumbers { get; set; }
    }
}
