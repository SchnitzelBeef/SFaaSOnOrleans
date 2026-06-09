using Infra.EventSchema;

namespace Infra.EcommerceFunctions
{

    public static class ProductFunctions
    {
        public static string GetProcessInventoryRequestFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id"),
                (typeof(Inventory), "inventory")
            });
            var code = $@"
                {args_code}
                var key = ""Product-"" + id;

                var request_customerId = (long)inventory.customerId;
                var request_price = (double)inventory.price;
                var request_quantity = (int)inventory.quantity;

                // Redis works with optimistic locking, so we retry the execution in the loop
                // If another thread modified the key concurrently until the key is unlocked
                while (true)
                {{
                    var product = kvs.Get<ProductState>(key);
                    var current_quantity = product.Quantity;
                    var current_price = product.Price;

                    // Check inventory quantity
                    if (current_quantity < request_quantity)
                    {{
                        // Add 90 units to the inventory if the current inventory is not enough
                        // We do not remove inventory in this case by assignment description

                        var newState = new ProductState {{Quantity = current_quantity + 90, Price = current_price}};
                        
                        // If this call succeeds, we can break the loop and continue
                        if (kvs.PutTransactional(key, newState, product))
                            break;
                    }}
                    else
                    {{

                        var newState = new ProductState {{Quantity = current_quantity - request_quantity, Price = current_price}};
                        
                        // If this call succeeds, we can break the loop and continue
                        if (kvs.PutTransactional(key, newState, product))
                            break;
                    }}
                }}

                // Get outcome stream and send OK balance message to analytics actor
                var outcomeEvent = new Outcome(request_customerId, id, request_price * request_quantity, Status.OK);
                return outcomeEvent;
            ";

            return code;
        }


        // These functions are not really necessary, since we can fetch the fields directly in the RedisKVS
        // It was added to assist in keeping original structure of the BDSOnlineStore
        public static string GetGetPriceFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id")
            });

            var code = $@"
                {args_code}
                var key = ""Product-"" + id;
                return kvs.Get<ProductState>(key).Price;
            ";

            return code;
        }

        public static string GetGetInventoryFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id")
            });

            var code = $@"
                {args_code}
                var key = ""Product-"" + id;
                return kvs.Get<ProductState>(key).Quantity;
            ";

            return code;
        }
    }
}
