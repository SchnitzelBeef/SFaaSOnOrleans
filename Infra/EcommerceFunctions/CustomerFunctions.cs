using Infra.EventSchema;

namespace Infra.EcommerceFunctions
{
    public static class CustomerFunctions
    {
        public static string GetProcessCheckoutFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id"),
                (typeof(Checkout), "checkout")
            });

            var code = $@"
                {args_code}
                var key = ""Customer-"" + id;

                var productId = (long)checkout.productId;
                var price = (double)checkout.price;
                var quantity = (int)checkout.quantity;

                // Redis works with optimistic locking, so we retry the execution in the loop
                // If another thread modified the key concurrently until the key is unlocked
                while (true)
                {{
                    var customer = kvs.Get<CustomerState>(key);
                    var total = price * quantity;
                    var balance = customer.Balance;
                    if (total > balance)
                    {{
                        // Get outcome log and send insufficient balance message to analytics actor      
                        var outcomeEvent = new Outcome(id, productId, total, Status.INSUFFICIENT_BALANCE);
                        return new Tuple<int, object>(0, outcomeEvent);
                    }}

                    // Construct a new state that we do not put yet in the KVS:
                    var newState = new CustomerState{{Balance = balance - total}};

                    // If this call succeeds, we can break the loop and continue
                    if (kvs.PutTransactional(key, newState, customer))
                        break; // success

                    // We could maybe add a sleep here if the above call fails
                }}
                var inventoryEvent = new Inventory(id, price, quantity);
                return new Tuple<int, object>(1, new object[] {{ productId, inventoryEvent }});
            ";

            return code;
        }

        // This function has absolutely no functionality currently
        public static string GetProcessOutcomeFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id"),
                (typeof(Outcome), "outcome") // outcome is not used for anything currently (since inventory is always in stock)
            });

            var code = $@"
                {args_code}
                // This function has absolutely no functionality currently
                return;
            ";

            return code;
        }

        // This function is not really necessary, since we can fetch the balance directly in the RedisKVS
        // It was added to assist in keeping original structure of the BDSOnlineStore
        public static string GetGetBalanceFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id")
            });

            var code = $@"
                    {args_code}
                    var key = ""Customer-"" + id;
                    return kvs.Get<CustomerState>(key).Balance;
            ";

            return code;
        }
    }
}
