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
                var key = ""Customer-"" + id;
                var request_price = (double)inventory.price;
                var request_quantity = (int)inventory.quantity;

                var current_quantity = kvs.Get<ProductState>(key).Quantity;
                var current_price = kvs.Get<ProductState>(key).Price;

                // Check inventory quantity
                if (current_quantity < request_quantity)
                {{
                    // Add 90 units to the inventory if the current inventory is not enough
                    // We do not remove inventory in this case by assignment description
                    kvs.Put(key, new ProductState{{Quantity = current_quantity + 90, Price = current_price}});
                }}
                else
                {{
                    kvs.Put(key, new ProductState{{Quantity = current_quantity - request_quantity, Price = current_price}});
                }}

                // Get outcome stream and send OK balance message to analytics actor   
                return ""Quantity withdrawn on product id: "" + id;
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
