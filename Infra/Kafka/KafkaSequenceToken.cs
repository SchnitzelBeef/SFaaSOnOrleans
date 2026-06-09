

namespace Infra.Kafka
{
    public class KafkaSequenceToken
    {
        public long Offset { get; protected set ; }

        public int EventIndex { get; protected set ; }

        public KafkaSequenceToken(long offset, int eventIndex)
        {
            this.Offset = offset;
            this.EventIndex = eventIndex;
        }
    }
}
