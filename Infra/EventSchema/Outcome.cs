using MessagePack;

namespace Infra.EventSchema
{
    [MessagePackObject(keyAsPropertyName: true)]
    public sealed class Outcome
    {
        public long customerId;
        public long productId;
        public double total;
        public Status status;

        [SerializationConstructor]
        public Outcome(long customerId, long productId, double total, Status status)
        {
            this.customerId = customerId;
            this.productId = productId;
            this.total = total;
            this.status = status;
        }
    }
}
