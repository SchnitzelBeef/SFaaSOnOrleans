using MessagePack;
using System.Text.Json;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class AnalyticsState : ICloneable
    {
        [Key(0)]
        public Dictionary<long, double> Query { get; set; }

        // Use a dictionary of hashSets so each Kafka Partition is a key into the the dictionary
        // Then we store the last Constants.StoreCapacity offsets from this partition to see if they have been processed before
        // This is the "Sliding window technique" and it is necessary for the analytics actor since this grain can be activate from different consumers using the same partition
        [Key(1)]
        public Dictionary<int, HashSet<long>> ProcessedOutcomeEvent { get; set; }

        [Key(2)]
        public Dictionary<long, int> DebugQuery { get; set; }

        public object Clone()
        {
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<AnalyticsState>(temp);
        }

        public AnalyticsState()
        {
            this.Query = new Dictionary<long, double>();
            // Currently not used
            this.DebugQuery = new Dictionary<long, int>();
            this.ProcessedOutcomeEvent = new Dictionary<int, HashSet<long>>(128);
        }
    }
}