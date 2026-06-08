using MessagePack;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class AnalyticsState
    {
        public Dictionary<long, double> Query { get; set; }
        public Dictionary<long, int> DebugQuery { get; set; }
        // Use a dictionary of hashSets so each Kafka Partition is a key into the the dictionary
        // Then we store the last Constants.StoreCapacity offsets from this partition to see if they have been processed before
        // This is the "Sliding window technique" and it is necessary for the analytics actor since this grain can be activate from different consumers using the same partition
        // public Dictionary<int, HashSet<long>> ProcessedOutcomeEvent { get; set; }
    }
}