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

                var total = price * quantity;
                var balance = kvs.Get<CustomerState>(key).Balance;
                if (total > balance)
                {{
                    // Get outcome log and send insufficient balance message to analytics actor      
                    var outcomeEvent = new Outcome(id, productId, total, Status.INSUFFICIENT_BALANCE);
                    return new object[] {{ id, outcomeEvent }};
                }}

                // Reserve balance
                // Not the prettiest way to update the state
                kvs.Put(key, new CustomerState{{Balance = balance - total}});

                var inventoryEvent = new Inventory(id, price, quantity);
                return new object[] {{ productId, inventoryEvent }};

                // If returning the 'Inventory' type does not work (due to serializer):
                // I believe it works with inventory, but this is a fallback method
                // return new object[] {{ productId, id, price, quantity }};
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
