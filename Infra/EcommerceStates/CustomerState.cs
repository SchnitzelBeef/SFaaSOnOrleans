using MessagePack;
using System.Text.Json;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class CustomerState
    {
        public double Balance { get; set; }
        // Event number to be able to ignore duplicates 
        public Dictionary<int, long> LastCheckoutEventOffset { get; set; }
        public Dictionary<int, long> LastOutcomeEventOffset { get; set; }
    }
}