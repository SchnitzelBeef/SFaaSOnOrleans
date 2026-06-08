using Infra.EventSchema;
namespace Infra.EcommerceFunctions
{

    public static class AnalyticsFunctions
    {

        public static string GetGetUpdateAsyncFunction()
        {

            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)>
            {
                // (typeof(long), "id"), // There is only "Analytics-0"
                (typeof(Outcome), "outcome")
            });

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                var customerId = (long)outcome.customerId;
                var productId = (long)outcome.productId;
                var total = (double)outcome.total;
                var status = (Status)Status.status;

                // If checkout is successful, update the total sales for the corresponding product
                if (status == Status.OK)
                {{
                    // We update a pointer so we most likely do not  have to perform kvs.Put here
                    var reference = kvs.Get<AnalyticsState>(key).Query; 
                    var previous = reference.GetValueOrDefault(customerId, 0);
                    reference[customerId] = previous + total;
                }}

                // ** Currently not implemented
                // Increment the debug counter for end-to-end latency metrics.
                // var previousCount = this.state.DebugQuery.GetValueOrDefault(outcome.customerId, 0);
                // this.state.DebugQuery[outcome.customerId] = previousCount + 1;

            ";

            return code;
        }

        public static string GetTop10Function()
        {
            // We only have one "actor" for analytics, so we don't accept parameter currently
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)> {});

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                return kvs.Get<AnalyticsState>(key).Query.OrderByDescending(kv => kv.Value).Take(10).ToList();
            ";

            return code;
        }

        // OBS, not implemented currently
        public static string GetCustomerOutcomeProcessedCountFunction()
        {
            return $@"return ""Debug Query not implemented, used in this call to CustomerOutcomeProcessedCount"";";

            // We only have one "actor" for analytics, so we don't accept parameter currently
            // var args_code = FunctionsHelper.GetArgs(new List<(Type, string)> {});
            // var code = $@"
            //         {args_code}
            //         var key = ""Analytics-0"";
            //         return kvs.Get<AnalyticsState>(key).Query.Sum();
            // ";
            // return code;

            // return this.state.DebugQuery.GetValueOrDefault(customerId, 0);
        }

        public static string GetGetSumOfAllBalanceFunction()

        {   
            // We only have one "actor" for analytics, so we don't accept parameter currently
            var args_code = FunctionsHelper.GetArgs(new List<(Type, string)> {});

            var code = $@"
                {args_code}
                var key = ""Analytics-0"";
                return kvs.Get<AnalyticsState>(key).Query.Sum();
            ";

            return code;
        }
    }
}
