using Infra.EventSchema;
namespace Infra.EcommerceFunctions
{

    public static class AnalyticsFunctions
    {
        public static string GetUpdateAsyncFunction()
        {
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                (typeof(Outcome), "outcome")
            });

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                var customerId = (long)outcome.customerId;
                var productId = (long)outcome.productId;
                var total = (double)outcome.total;
                var status = (Status)outcome.status;

                var state = kvs.Get<AnalyticsState>(key);                

                // If checkout is successful, update the total sales for the corresponding product
                if (status == Status.OK)
                {{
                    var previous = state.Query.GetValueOrDefault(customerId, 0);
                    state.Query[customerId] = previous + total;
                }}

                // Increment the debug counter for end-to-end latency metrics.
                var debugPrevious = state.DebugQuery.GetValueOrDefault(customerId, 0);
                state.DebugQuery[customerId] = debugPrevious + 1;

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
