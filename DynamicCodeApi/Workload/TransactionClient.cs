using Controller;
using Infra.EcommerceStates;
using Infra.Interfaces;
using System.Diagnostics;
using Workload;

namespace DynamicCodeApi.Workload;


internal class TransactionClient
{
    private int numCustomerActor = 10;
    private int numProductActor = 100;

    private int numEpochs = 1;
    private int numWarmupEpochs = 0;

    // for experiment setting
    private int numCustomerThread = 8;
    private TimeSpan runTime = TimeSpan.FromMilliseconds(1000);    // use this time to control how long time the experiment will run

    private CountdownEvent allThreadsStart;
    private CountdownEvent allThreadsAreDone;

    private RedisKVS redisKVS;
    private CodeController controller;

    // Start + end ts per transaction per thread.
    private List<List<Tuple<long, long>>> threadTimestamps;

    public TransactionClient()
    {
        this.redisKVS = new RedisKVS(null);
        this.controller = new CodeController(this.redisKVS);
    }

    public async Task RunClient()
    {
        // Start + end ts per transaction per thread per epoch.
        var allTimestamps = new List<List<List<Tuple<long, long>>>>();

        var workload = new WorkloadGenerator(numCustomerActor, numProductActor, this.controller);
        for (int epoch = 0; epoch < numEpochs + numWarmupEpochs; epoch++)
        {
            // ================================================================================================================
            // STEP 0: publish all actor functions to the RedisKVS
            Console.WriteLine("\n ***********************************************************************");
            await workload.InitAllActorFunctions();

            // ================================================================================================================
            // STEP 1: init all actors
            await workload.InitAllActors();
            Console.WriteLine("\n ***********************************************************************");
            Console.WriteLine($"#customer = {numCustomerActor}, #product = {numProductActor}");

            // ================================================================================================================
            // STEP 2: get initial inventory of all products
            var before_totalAmount = (await workload.GetAllInventory()).Item1.Sum();
            Console.WriteLine($"Before total amount: {before_totalAmount}");

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
            var res = await workload.GetAllInventory();
            var inventory = res.Item1;
            var hasEverGotNegativeInventory = res.Item2;
            Console.WriteLine("\n ***********************************************************************");
            if (hasEverGotNegativeInventory) Console.WriteLine($"The inventory has once become negative!!!");
            Console.WriteLine("\n ***********************************************************************");

            // wait for a while to make sure all transactions are done
            Thread.Sleep(5000);

            // the top-10 customers
            Console.WriteLine($"The top-10 customers are: ");
            var top10 = await workload.GetTopTen();
            Console.WriteLine(top10);
            Console.WriteLine("\n ***********************************************************************");
        }
        await this.controller.clientManager.StopClient();
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
        File.WriteAllLines($"../latencies_{numCustomerActor}-{numProductActor}.csv", lines);
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
        var totalTime = watch.Elapsed.TotalMilliseconds;

        Console.WriteLine($"Thread {thread}: " +
                            $"Number of transactions emitted = {numEmitTransaction} " +
                            $"Total time elapsed = {totalTime}");
        allThreadsAreDone.Signal();
        await workload.StopClient();
    }

    private async void EndToEndLatencySampler(object obj)
    {
        var customerId = (long)1e9;
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
                var key = "Analytics-0";
                newCount = this.redisKVS.Get<AnalyticsState>(key).DebugQuery.GetValueOrDefault(customerId, 0);
                if (newCount > currentCount || watch.Elapsed >= runTime)
                {
                    break;
                }
                Thread.Sleep(100);
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
