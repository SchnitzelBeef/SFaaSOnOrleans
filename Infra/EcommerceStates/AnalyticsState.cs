using MessagePack;

namespace Infra.EcommerceStates
{
    [MessagePackObject]
    public class AnalyticsState
    {
        public Dictionary<long, double> Query { get; set; }
        public Dictionary<long, int> DebugQuery { get; set; }
        // Event number to be able to ignore duplicates 
        public Dictionary<int, long> LastOutcomeEventOffset { get; set; }
    }
}