using MessagePack;

namespace ECommerce.Olep.Schema
{
    [MessagePackObject]
    public sealed class Outcome
    {
        [Key(0)]
        public readonly long customerId;
        [Key(1)]
        public readonly long productId;
        [Key(2)]
        public readonly double total;
        [Key(3)]
        public readonly Status status;

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
