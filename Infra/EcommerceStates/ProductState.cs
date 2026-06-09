using MessagePack;
using System.Text.Json;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class ProductState
    {
        public int Quantity { get; set; }
        public double Price { get; set; }

        // Event number to be able to ignore duplicates 

        public Dictionary<int, long> LastInventoryEventOffset { get; set; }
    }
}
