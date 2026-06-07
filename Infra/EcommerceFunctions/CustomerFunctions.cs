

namespace Infra.EcommerceFunctions
{
    public static class CustomerFunctions
    {

        public static string GetProcessCheckout()
        {
            var wrapped_code = $@"
                if (args.Length < 2) {{
                    return ""Expected 2 arguments, got "" + args.Length;
                }}
                if (args[0] is not long id) {{
                    return ""Expected args[0] to be type 'long', got "" + args[0]?.GetType().Name;
                }}
                if (args[1] is not Checkout checkout) {{
                    return ""Expected args[1] to be type 'Checkout', got "" + args[1]?.GetType().Name;
                }}

                var key = ""Customer-"" + id;
                var price = (double)checkout.price;
                var quantity = (int)checkout.quantity;

                var total = price * quantity;
                var balance = kvs.Get<CustomerState>(key).Balance;
                if (total > balance)
                {{
                    // Get outcome log and send insufficient balance message to analytics actor            
                    // var outcome = new Outcome(this.id, productId, price * quantity, Status.INSUFFICIENT_BALANCE);
                    //await outcomeProducer.Append(this.id, outcome);
                    // _ = outcomeProducer.Append(this.id, outcome);

                    return ""Insufficient balance on customer id: "" + id;
                }}

                // Reserve balance
                // Not the prettiest way to update the state
                kvs.Put(key, new CustomerState{{Balance = balance - total}});

                // Not done currently, since other actor functions are not implemented
                return ""Balance withdrawn on customer id: "" + id;
            ";

            return wrapped_code;


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

        // public Task<double> GetBalance()
        // {
        //     return Task.FromResult(this.state.Balance);
        // }
    }
}
