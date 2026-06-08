using Controller;
using Infra.Interfaces;
using System.Diagnostics;
using Workload;
using System.Diagnostics;

namespace DynamicCodeApi.Workload
{
    internal class TransactionClient
    {
        // These are the three variables we modify during evaluation
        // I.e. we modify only the amount of actors 
        private int numProxyActor = 10;
        private int numCustomerActor = 10;
        private int numProductActor = 100;

        private int numEpochs = 10;
        private int numWarmupEpochs = 2;

        // for experiment setting
        private int numCustomerThread = 8;
        private TimeSpan runTime = TimeSpan.FromSeconds(10);    // use this time to control how long time the experiment will run

        private CountdownEvent allThreadsStart;
        private CountdownEvent allThreadsAreDone;
        
        // Start + end ts per transaction per thread.
        private List<List<Tuple<long, long>>> threadTimestamps;

        private RedisKVS redisKVS;
        private CodeController controller; 

        public TransactionClient(IClusterClient client)
        {
            this.redisKVS = new RedisKVS(null);
            this.controller = new CodeController(this.redisKVS, client);
        }

        public async Task RunClient()
        {
            // Start + end ts per transaction per thread per epoch.
            var allTimestamps = new List<List<List<Tuple<long, long>>>>();
            // numCustomerActor + 1 for the extra sample actor that we use to check end-to-end latency
            var workload = new WorkloadGenerator(numCustomerActor + 1, numProductActor, this.controller); // Currently not using 'numProxyActor'
            
            // ================================================================================================================
            // STEP 0: publish all actor functions to the RedisKVS
            Console.WriteLine("\n ***********************************************************************");
            await workload.InitAllActorFunctions();
            
            for (int epoch = 0; epoch < numEpochs + numWarmupEpochs; epoch++)
            {
                Console.WriteLine($"\n\n============================== Epoch {epoch} ==============================");

                

                // ================================================================================================================
                // STEP 1: init all actors
                await workload.InitAllActors();
                Console.WriteLine("\n ***********************************************************************");
                Console.WriteLine($"#customer = {numCustomerActor}, #product = {numProductActor}");

                // ================================================================================================================
                // STEP 2: get initial inventory of all products
                var beforeTotalAmount = (await workload.GetAllInventory()).Item1.Sum();
                var beforeTotalBalance = (await workload.GetAllBalance()).Item1.Sum();

                // ================================================================================================================
                // STEP 3: spawn multiple threads to submit transactions
                allThreadsStart = new CountdownEvent(numCustomerThread + 1);
                allThreadsAreDone = new CountdownEvent(numCustomerThread + 1);
                Console.WriteLine("\n ***********************************************************************");
                Console.WriteLine($"Spawning {numCustomerThread} threads to check-out order");

                threadTimestamps = new List<List<Tuple<long, long>>>();
                for (int i = 0; i < numCustomerThread; i++)
                {
                    threadTimestamps.Add(new List<Tuple<long, long>>());
                    var thread = new Thread(CustomerWorkAsync);
                    thread.Start(i);
                }

                threadTimestamps.Add(new List<Tuple<long, long>>());
                var samplerThread = new Thread(EndToEndLatencySampler);
                samplerThread.Start();
                allThreadsAreDone.Wait();   // wait until all threads are done

                if (epoch >= numWarmupEpochs)
                {
                    allTimestamps.Add(threadTimestamps);
                }

                // ================================================================================================================
                // STEP 4: check inventory of all products again
                var invRes = await workload.GetAllInventory();
                var afterTotalAmount = invRes.Item1.Sum();
                Console.WriteLine("\n ***********************************************************************");
                Console.WriteLine($"Before total product quantity {beforeTotalAmount}");
                Console.WriteLine($"After total product quantity  {afterTotalAmount}");
                Console.WriteLine("\n ***********************************************************************");
                if (invRes.Item2)
                {
                    Console.WriteLine($"The inventory has once become negative!!!");
                    Console.WriteLine("\n ***********************************************************************");
                }

                var balRes = await workload.GetAllBalance();
                var afterTotalBalance = balRes.Item1.Sum();
                var totalSpent = beforeTotalBalance - afterTotalBalance;
                Console.WriteLine($"Before total customer balance {beforeTotalBalance}");
                Console.WriteLine($"After total customer balance  {afterTotalBalance}");
                Console.WriteLine($"Total customer balance spent  {totalSpent}");
                Console.WriteLine("\n ***********************************************************************");
                if (balRes.Item2)
                {
                    Console.WriteLine($"A customers balance has once become negative!!!");
                    Console.WriteLine("\n ***********************************************************************");
                }


                // the top-10 customers
                Console.WriteLine($"The top-10 customers are: ");
                var top10 = await workload.GetTopTen();
                Console.WriteLine(top10);

                //var totalBalanceAnalytics = await workload.GetTotalAnalyticsBalance();
                //Console.WriteLine($"Sum of analytics (Should match 'Total customer balance spent'): {totalBalanceAnalytics}");
                //Console.WriteLine("\n ***********************************************************************");
            }

            await workload.StopClient();
            await WriteCSV(allTimestamps);
            Console.WriteLine("The experiment is done. ");
        }

        private async Task WriteCSV(List<List<List<Tuple<long, long>>>> latencies)
        {
            var maxLen = latencies.Select(l => l.Select(l => l.Count).Max()).Max();

            var header = "epoch,thread";
            for (int i = 0; i < maxLen; i++)
            {
                header += $",start{i}_ns,end{i}_ns";
            }

            var lines = new List<string>() { header };
            for (int epoch = 0; epoch < latencies.Count; epoch++)
            {
                for (int thread = 0; thread < latencies[epoch].Count; thread++)
                {
                    var row = new List<string> { epoch.ToString(), thread.ToString() };
                    for (int i = 0; i < latencies[epoch][thread].Count; i++)
                    {
                        row.Add(latencies[epoch][thread][i].Item1.ToString());
                        row.Add(latencies[epoch][thread][i].Item2.ToString());
                    }
                    lines.Add(string.Join(",", row));
                }
            }
            File.WriteAllLines($"{numProxyActor}-{numCustomerActor}-{numProductActor}_latencies.csv", lines);
        }

        // ================================================================================================================
        private async void CustomerWorkAsync(object obj)
        {
            var thread = (int)obj;
            var numEmitTransaction = 0;
            var workload = new WorkloadGenerator(numCustomerActor, numProductActor, this.controller); 
            var watch = new Stopwatch();
            var thisThreadLatencies = threadTimestamps[thread];

            allThreadsStart.Signal();
            allThreadsStart.Wait();      // make sure all threads start at the same time

            watch.Start();
            while (watch.Elapsed < runTime)
            {
                numEmitTransaction++;
                var startTime = (long)(watch.ElapsedTicks * 1e9 / Stopwatch.Frequency);
                await workload.NewOrder();
                var endTime = (long)(watch.ElapsedTicks * 1e9 / Stopwatch.Frequency);
                thisThreadLatencies.Add(new(startTime, endTime));
            }
            watch.Stop();

            Console.WriteLine($"Thread {thread}: " +
                              $"Number of transactions emitted = {numEmitTransaction} " +
                              $"Total time elapsed = {watch.Elapsed}");
            allThreadsAreDone.Signal();
            await workload.StopClient();
        }


        private async void EndToEndLatencySampler(object obj)
        {
            var customerId = numCustomerActor + 1;
            var numEmitTransaction = 0;
            var workload = new WorkloadGenerator(1, 1, this.controller);
            var watch = new Stopwatch();
            var thisThreadLatencies = threadTimestamps[numCustomerThread];
            var currentCount = 0;
            var newCount = 0;

            allThreadsStart.Signal();
            allThreadsStart.Wait();      // make sure all threads start at the same time

            watch.Start();
            while (watch.Elapsed < runTime)
            {
                numEmitTransaction++;
                var startTime = (long)(watch.ElapsedTicks * 1e9 / Stopwatch.Frequency);
                await workload.NewCheckOutOrder(customerId);
                while (true)
                {
                    newCount = await workload.GetCustomerProcessedCount(customerId);
                    if (newCount > currentCount)
                    {
                        break;
                    }
                    Thread.Sleep(10);
                }
                currentCount = newCount;
                var endTime = (long)(watch.ElapsedTicks * 1e9 / Stopwatch.Frequency);
                thisThreadLatencies.Add(new(startTime, endTime));
            }
            watch.Stop();

            Console.WriteLine($"Thread S: " +
                              $"Number of transactions emitted = {numEmitTransaction} " +
                              $"Total time elapsed = {watch.Elapsed}");
            allThreadsAreDone.Signal();
            await workload.StopClient();
        }
    }
}
