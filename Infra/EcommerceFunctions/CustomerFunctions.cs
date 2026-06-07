

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
                    // var outcome = new Outcome(this.id, productId, price * quantity, Status.INSUFFICIENT_BALANCE);
                    // await outcomeProducer.Append(this.id, outcome);
                    // _ = outcomeProducer.Append(this.id, outcome);
                    // OBS
                    // return ""Insufficient balance on customer id: "" + id;
                }}

                // Reserve balance
                // Not the prettiest way to update the state
                kvs.Put(key, new CustomerState{{Balance = balance - total}});

                // I cannot get it to return the Inventory type
                // var inventoryEvent = new Inventory(id, price, quantity);
                return new object[] {{ productId, id, price, quantity }};
            ";

            return code;


            // // Get product log and send inventory request to product actor
            // var inventoryEvent = new Inventory(this.id, checkout.price, checkout.quantity);
            // //await inventoryProducer.Append(checkout.productId, inventoryEvent);

            // _ = inventoryProducer.Append(checkout.productId, inventoryEvent);

            // // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
            // if (token is KafkaSequenceToken _concreteToken)
            // {
            //     if (state.LastCheckoutEventSequenceNumbers.ContainsKey(_concreteToken.EventIndex))
            //     {
            //         state.LastCheckoutEventSequenceNumbers[_concreteToken.EventIndex] = _concreteToken.SequenceNumber;
            //     }
            //     else
            //     {
            //         state.LastCheckoutEventSequenceNumbers.Add(_concreteToken.EventIndex, _concreteToken.SequenceNumber);
            //     }
            // }
        }

        // public async Task ProcessOutcome(Outcome outcome, StreamSequenceToken token = null)
        // {
        //     if (token is KafkaSequenceToken concreteToken)
        //     {
        //         // Check if the event is a duplicate by comparing the sequence number (timestamp) with the last processed event
        //         if (this.state.LastOutcomeEventSequenceNumbers.TryGetValue(concreteToken.EventIndex, out long lastSequenceNumber))
        //         {
        //             if (concreteToken.SequenceNumber <= lastSequenceNumber)
        //             {
        //                 // If so, just write to console and return 
        //                 // Console.WriteLine($"Duplicate event detected for customer actor {this.id} in outcome processing");
        //                 return;
        //             }
        //         }
        //     }

        //     // Realistically we should undo the reservation here in case the outcome failed,
        //     // however the outcome cannot fail after INSUFFICIENT_BALANCE has been checked,
        //     // (assuming at least once delivery), so just ignore for now.
        //     // Again, we cannot implement at least once as that requires deduplication,
        //     // but we cannot add an id to the event to distinguish them.

        //     // If, for example, the inventory could not resupply,
        //     // then we would need to undo the reservation here.

        //     // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
        //     if (token is KafkaSequenceToken _concreteToken)
        //     {
        //         if (state.LastOutcomeEventSequenceNumbers.ContainsKey(_concreteToken.EventIndex))
        //         {
        //             state.LastOutcomeEventSequenceNumbers[_concreteToken.EventIndex] = _concreteToken.SequenceNumber;
        //         }
        //         else
        //         {
        //             state.LastOutcomeEventSequenceNumbers.Add(_concreteToken.EventIndex, _concreteToken.SequenceNumber);
        //         }
        //     }
        // }

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
