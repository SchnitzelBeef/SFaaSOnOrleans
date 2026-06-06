using MessagePack;
using System.Text.Json;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class CustomerState : ICloneable
    {
        [Key(0)]
        public double Balance { get; set; }
        // Event number to be able to ignore duplicates 
        [Key(1)]
        public Dictionary<int, long> LastCheckoutEventSequenceNumbers { get; set; }
        [Key(2)]
        public Dictionary<int, long> LastOutcomeEventSequenceNumbers { get; set; }
        public object Clone()
        {
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<CustomerState>(temp);
        }

        public CustomerState(double balance)
        {
            this.Balance = balance;
            // Currently not used
            this.LastCheckoutEventSequenceNumbers = new Dictionary<int, long>();
            this.LastOutcomeEventSequenceNumbers = new Dictionary<int, long>();
        }
    }
}