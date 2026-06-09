using Infra.EventSchema;
using Infra.Kafka;

namespace Infra.EcommerceFunctions
{
    public static class CustomerFunctions
    {
        public static string GetProcessCheckoutFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "id"),
                (typeof(Checkout), "checkout"),
                (typeof(KafkaSequenceToken), "token"),
            });

            var code = $@"
                {args_code}
                var key = ""Customer-"" + id;
                var state = kvs.Get<CustomerState>(key);

                // Check for null arguments
                if (state == null)
                {{
                    return new Tuple<int, object>(-2, $""CustomerState is null for key "" + key);
                }}

                var _LastCheckoutEventOffset = state.LastCheckoutEventOffset;
                if (_LastCheckoutEventOffset == null)
                {{
                    return new Tuple<int, object>(-3, $""_LastCheckoutEventOffset is null for key "" + key);
                }}

                // Check for duplicate events to ensure exactly once
                if (_LastCheckoutEventOffset.TryGetValue(token.EventIndex, out long lastOffset))
                {{
                    if (token.Offset <= lastOffset)
                    {{
                        // If so, just return with -1 to notify composition that duplicate event was detected
                        return new Tuple<int, object>(-1, ""ProcessCheckout on key: "" + key);
                    }}
                }}


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
                    var newState = new CustomerState{{
                        Balance = balance - total,
                        LastCheckoutEventOffset = _LastCheckoutEventOffset
                    }};

                    // If this call succeeds, we can break the loop and continue
                    if (kvs.PutTransactional(key, newState, customer))
                        break; // success

                    // We could maybe add a sleep here if the above call fails
                }}

                var inventoryEvent = new Inventory(id, price, quantity);

                // The request has now been processed fully and we note the offset of the token to be able to ignore duplicates later
                if (_LastCheckoutEventOffset.ContainsKey(token.EventIndex))
                {{
                    _LastCheckoutEventOffset[token.EventIndex] = token.Offset;
                }}
                else
                {{
                    _LastCheckoutEventOffset.Add(token.EventIndex, token.Offset);
                }}
                  
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
                (typeof(Outcome), "outcome"), // outcome is not used for anything currently (since inventory is always in stock)
                (typeof(KafkaSequenceToken), "token")
            });

            var code = $@"
                // This function has absolutely no functionality currently, except check for duplicate events
                // This is because the inventory can never 'fail' in business logic, because the product is always in stock
                // We should reinsert the balance into the account in case the product can return something like 'Insufficient stock'
                
                {args_code}
                var key = ""Customer-"" + id;
                var state = kvs.Get<CustomerState>(key);

                // Check for null arguments
                if (state == null)
                {{
                    return new Tuple<int, object>(-2, $""CustomerState is null for key "" + key);
                }}

                var _LastOutcomeEventOffset = state.LastOutcomeEventOffset;
                if (_LastOutcomeEventOffset == null)
                {{
                    return new Tuple<int, object>(-3, $""_LastOutcomeEventOffset is null for key "" + key);
                }}

                // Check for duplicate events to ensure exactly once
                var _LastOutcomeEventOffset = kvs.Get<CustomerState>(key)._LastOutcomeEventOffset;
                if (_LastOutcomeEventOffset.TryGetValue(token.EventIndex, out long lastOffset))
                {{
                    if (token.Offset <= lastOffset)
                    {{
                        // If so, just return with -1 to notify composition that duplicate event was detected
                        return new Tuple<int, object>(-1, ""ProcessInventoryRequest on key: "" + key);
                    }}
                }}
                
                // The request has now been processed fully and we note the offset of the token to be able to ignore duplicates later
                if (_LastOutcomeEventOffset.ContainsKey(token.EventIndex))
                {{
                    _LastOutcomeEventOffset[token.EventIndex] = token.Offset;
                }}
                else
                {{
                    _LastOutcomeEventOffset.Add(token.EventIndex, token.Offset);
                }}

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
