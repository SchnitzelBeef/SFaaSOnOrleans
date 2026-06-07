// namespace Infra.EcommerceFunctions
// {

//     public static class AnalyticsFunctions
//     {

//         public static string GetGetUpdateAsyncFunction()
//         {
//             // If checkout is successful, update the total sales for the corresponding product
//             if (outcome.status == Status.OK)
//             {
//                 var previous = this.state.Query.GetValueOrDefault(outcome.customerId, 0);
//                 this.state.Query[outcome.customerId] = previous + outcome.total;
//             }

//             // Increment the debug counter for end-to-end latency metrics.
//             var previousCount = this.state.DebugQuery.GetValueOrDefault(outcome.customerId, 0);
//             this.state.DebugQuery[outcome.customerId] = previousCount + 1;

//             // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
//             if (token is KafkaSequenceToken _concreteToken)
//             {
//                 if (state.ProcessedOutcomeEvent.ContainsKey(_concreteToken.EventIndex))
//                 {

//                     while (state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Count() >= Constants.StoreCapacity)
//                     {
//                         long oldestOffset = state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Min();
//                         state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Remove(oldestOffset);
//                     }
//                     state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Add(_concreteToken.SequenceNumber);
//                 }
//                 else
//                 {
//                     state.ProcessedOutcomeEvent.Add(
//                         _concreteToken.EventIndex,
//                         new HashSet<long>(Constants.StoreCapacity)
//                         {
//                             _concreteToken.SequenceNumber
//                         }
//                     );
//                 }
//             }
//             return Task.CompletedTask;
//         }

//         public async Task<List<KeyValuePair<long, double>>> Top10()
//         {
//             // Get top ten customers by their value
//             var top10 = this.state.Query.OrderByDescending(kv => kv.Value).Take(10).ToList();
//             return await Task.FromResult(top10);
//         }

//         public async Task<int> CustomerOutcomeProcessedCount(long customerId)
//         {
//             return this.state.DebugQuery.GetValueOrDefault(customerId, 0);
//         }

//         public async Task<double> GetSumOfAllBalance()
//         {
//             return await Task.FromResult(this.state.Query.Values.Sum());
//         }
//     }
// }
