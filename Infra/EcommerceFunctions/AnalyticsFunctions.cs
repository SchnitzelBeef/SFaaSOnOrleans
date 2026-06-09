using Infra.EventSchema;
using Infra.Kafka;

namespace Infra.EcommerceFunctions
{

    public static class AnalyticsFunctions
    {
        public static string GetUpdateAsyncFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(Outcome), "outcome"),
                (typeof(KafkaSequenceToken), "token")
            });

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                var state = kvs.Get<AnalyticsState>(key);

                // Check for null arguments
                if (state == null)
                {{
                    return new Tuple<int, object>(-2, $""AnalyticsState is null for key "" + key);
                }}

                var _LastOutcomeEventOffset = state.LastOutcomeEventOffset;
                if (_LastOutcomeEventOffset == null)
                {{
                    return new Tuple<int, object>(-3, $""_LastOutcomeEventOffset is null for key "" + key);
                }}

                // Check for duplicate events to ensure exactly once
                if (_LastOutcomeEventOffset.TryGetValue(token.EventIndex, out long lastOffset))
                {{
                    if (token.Offset <= lastOffset)
                    {{
                        // If so, just return with -1 to notify composition that duplicate event was detected
                        return new Tuple<int, object>(-1, ""ProcessInventoryRequest on key: "" + key);
                    }}
                }}

                var customerId = (long)outcome.customerId;
                var productId = (long)outcome.productId;
                var total = (double)outcome.total;
                var status = (Status)outcome.status;         

                // If checkout is successful, update the total sales for the corresponding product
                if (status == Status.OK)
                {{
                    var previous = state.Query.GetValueOrDefault(customerId, 0);
                    state.Query[customerId] = previous + total;
                }}

                // Increment the debug counter for end-to-end latency metrics.
                var debugPrevious = state.DebugQuery.GetValueOrDefault(customerId, 0);
                state.DebugQuery[customerId] = debugPrevious + 1;

                // The request has now been processed fully and we note the offset of the token to be able to ignore duplicates later
                if (_LastOutcomeEventOffset.ContainsKey(token.EventIndex))
                {{
                    _LastOutcomeEventOffset[token.EventIndex] = token.Offset;
                }}
                else
                {{
                    _LastOutcomeEventOffset.Add(token.EventIndex, token.Offset);
                }}

                return kvs.Put<AnalyticsState>(key, state);                
            ";

            return code;
        }

        public static string GetTop10Function()
        {
            // We only have one "actor" for analytics, so we don't accept parameter currently
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)> { });

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                return kvs.Get<AnalyticsState>(key).Query.OrderByDescending(kv => kv.Value).Take(10).ToList();
            ";

            return code;
        }

        public static string GetCustomerOutcomeProcessedCountFunction()
        {
            // We only have one "actor" for analytics, so we don't accept parameter currently
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(long), "customerId")
            });

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                return kvs.Get<AnalyticsState>(key).DebugQuery.GetValueOrDefault(customerId, 0);
            ";

            return code;
        }

        public static string GetGetSumOfAllBalanceFunction()
        {
            // We only have one "actor" for analytics, so we don't accept parameter currently
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)> { });

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                return kvs.Get<AnalyticsState>(key).Query.Sum();
            ";

            return code;
        }
    }
}
