using System.Diagnostics;
using Workload;
using Controller;
using Infra.Interfaces;
using Infra.Service;
using DynamicCodeApi;

namespace DynamicCodeApi.Workload;

internal class TransactionClient
{

    int numCustomerActor = 10;
    int numProductActor = 100;

    // for experiment setting
    int numCustomerThread = 8;
    TimeSpan runTime = TimeSpan.FromSeconds(10);    // use this time to control how long time the experiment will run

    CountdownEvent allThreadsStart;
    CountdownEvent allThreadsAreDone;

    public async Task RunClient()
    {

        // ================================================================================================================
        // STEP 1: init all actors
        var workload = new WorkloadGenerator(numCustomerActor, numProductActor);
        await workload.InitAllActors();
        Console.WriteLine("\n ***********************************************************************");
        Console.WriteLine($"#customer = {numCustomerActor}, #product = {numProductActor}");

        // ================================================================================================================
        // STEP 2: get initial inventory of all products
        var before_totalAmount = (await workload.GetAllInventory()).Item1.Sum();

        // ================================================================================================================
        // STEP 3: spawn multiple threads to submit transactions
        allThreadsStart = new CountdownEvent(numCustomerThread);
        allThreadsAreDone = new CountdownEvent(numCustomerThread);
        Console.WriteLine("\n ***********************************************************************");
        Console.WriteLine($"Spawning {numCustomerThread} threads to check-out order");
        for (int i = 0; i < numCustomerThread; i++)
        {
            var thread = new Thread(CustomerWorkAsync);
            thread.Start(i);
        }

        allThreadsAreDone.Wait();   // wait until all threads are done

        // ================================================================================================================
        // STEP 4: check inventory of all products again
        var res = await workload.GetAllInventory();
        var inventory = res.Item1;
        var hasEverGotNegativeInventory = res.Item2;
        Console.WriteLine("\n ***********************************************************************");
        if (hasEverGotNegativeInventory) Console.WriteLine($"The inventory has once become negative!!!");
        Console.WriteLine("\n ***********************************************************************");

        // the top-10 customers
        Console.WriteLine($"The top-10 customers are: ");
        var top10 = await workload.GetTopTen();
        Console.WriteLine(top10);
        Console.WriteLine("\n ***********************************************************************");

        Console.WriteLine("The experiment is done. ");
    }

    public async Task RunTest()
    {
        // Added for initial tests

        var redisKVS = new RedisKVS(null); // Pass null for logger in this example
        var controller = new CodeController(redisKVS);

        var foo = new CodeRegistrationRequest
        {
            FunctionName = "AddNumbers",
            Code = "return (System.Int64)args[0] + (System.Int64)args[1];"
        };

        Console.WriteLine(controller.RegisterFunction(foo)); 

        Thread.Sleep(5000); // wait for the HTTP server to start

        Console.WriteLine($"Adding");


        var invoke = new FunctionExecutionRequest
        {
            FunctionName = "AddNumbers",
            Parameters = new object[] { 10L, 20L }
        };


        var res = controller.TestFunction(invoke);

        await res;

        Console.WriteLine($"Execution result: {res.Result}");

        // var test = new TestGenerator();
        // await test.InitAllActors();
    }


    // ================================================================================================================
    async void CustomerWorkAsync(object obj)
    {
        var thread = (int)obj;
        var numEmitTransaction = 0;
        var workload = new WorkloadGenerator(numCustomerActor, numProductActor);
        var watch = new Stopwatch();

        allThreadsStart.Signal();
        allThreadsStart.Wait();      // make sure all threads start at the same time

        watch.Start();
        while (watch.Elapsed < runTime)
        {
            numEmitTransaction++;
            await workload.NewCheckOutOrder();   // submit one transaction a time
        }
        var totalTime = watch.Elapsed.TotalMilliseconds;

        Console.WriteLine($"Thread {thread}: " +
                            $"Number of transactions emitted = {numEmitTransaction} " +
                            $"Total time elapsed = {totalTime}");
        allThreadsAreDone.Signal();
    }
     
}


