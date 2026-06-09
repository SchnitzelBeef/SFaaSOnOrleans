using DynamicCodeApi;
using DynamicCodeApi.Workload;
using Infra;
using Infra.Interfaces;
using Infra.Kafka;

var sharedClientManager = new OrleansClientManager();
var sharedClient = await sharedClientManager.StartClient();

ThreadStart work = () =>
{
    var builder = WebApplication.CreateBuilder(args);

    // Add KVS implementation to the container
    builder.Services.AddSingleton<IKeyValueStore, RedisKVS>();
    builder.Services.AddSingleton(sharedClient); // Register the shared Orleans client
    // Add services to the container
    builder.Services.AddControllers().AddNewtonsoftJson();
    builder.Services.AddSwaggerGen();

    var app = builder.Build();

    // Configure the HTTP request pipeline.
    if (app.Environment.IsDevelopment())
    {
        app.UseSwagger();
        app.UseSwaggerUI();
    }

    app.MapControllers();

    app.Run();
};

Console.WriteLine("Starting the transaction client HTTP app...");

Thread thread = new Thread(work);
thread.Start();

// wait for the HTTP server to start, this is probably not the best way to do this
Thread.Sleep(5000);

//obs, temporary setup
// only uses single mediator grain - should be divided into proper Kafka partitions and Topics
// and initiated inside transaction client/workload generator  
var mediatorGrain = sharedClient.GetGrain<IMediatorGrain>("mediator");
await mediatorGrain.Init(Constants.CheckoutNamespace, Constants.CheckoutTopicGroup, 0, -1);

/*

// Test-setup, should not be part of final hand in
// Just used to test that we can submit functions to the Redis KVS and execute them through the mediator grain
Console.WriteLine("Initializing test...");

var redisKVS = new RedisKVS(null); // Null for logger
var controller = new CodeController(redisKVS);

// For debugging. TODO Remove later
// redisKVS.Reset();

var foo = new CodeRegistrationRequest
{
    FunctionName = "AddNumbers",
    Code = "return (System.Int64)args[0] + (System.Int64)args[1];"
};

var bar = new CodeRegistrationRequest
{
    FunctionName = "Increment",
    Code = "return (System.Int64)args[0] + 1;"
};

var complex = new CodeRegistrationRequest
{
    FunctionName = "Branched",
    Code = "if ((System.Int64)args[0] == 0) { return new System.Tuple<int, object>(0, 100L); } else { return new System.Tuple<int, object>(1, 123L); }"
};

var composedFunc = new FunctionCompositionRequest
{
    FunctionName = "AddThenIncrement",
    Root = new CompositionAST
    {
        FunctionName = "AddNumbers",
        ChildrenInOrder = new CompositionAST[]
        {
            new CompositionAST
            {
                FunctionName = "Increment",
                ChildrenInOrder = new CompositionAST[]
                {
                    new CompositionAST
                    {
                        FunctionName = "Increment",
                        ChildrenInOrder = new CompositionAST[] { },
                    },
                }
            },
        }
    }
};

var composedFunc2 = new FunctionCompositionRequest
{
    FunctionName = "BranchedComposition",
    Root = new CompositionAST
    {
        FunctionName = "Branched",
        ChildrenInOrder = new CompositionAST[]
        {
            // First branch calls the branched again.
            new CompositionAST
            {
                FunctionName = "Branched",
                ChildrenInOrder = new CompositionAST[] { },
            },

            // Second branch is null, so just return, no workflow
            null,
        }
    }
};

// This does not work. Should we allow composing of compositions?
var composedFunc3 = new FunctionCompositionRequest
{
    FunctionName = "AddThenIncrementAgain",
    Root = new CompositionAST
    {
        FunctionName = "AddThenIncrement",
        ChildrenInOrder = new CompositionAST[] {
            new CompositionAST {
                FunctionName = "Increment",
                ChildrenInOrder = new CompositionAST[] { },
            }
        },
    }
};

Console.WriteLine($"Function registered: {controller.RegisterFunction(foo)}");
Console.WriteLine($"Function registered: {controller.RegisterFunction(bar)}");
Console.WriteLine($"Function composed: {controller.RegisterComposition(composedFunc)}");
Console.WriteLine($"Function composed: {controller.RegisterComposition(composedFunc2)}");
Console.WriteLine($"Function registered: {controller.RegisterFunction(foo)}");
Console.WriteLine($"Function registered: {controller.RegisterFunction(bar)}");
Console.WriteLine($"Function composed: {controller.RegisterComposition(composedFunc)}");
var simple = new CodeRegistrationRequest
{
    FunctionName = "RetType",
    Code = "return new Inventory(1, 123.123, 2);",
};

var simpleComp = new FunctionCompositionRequest
{
    FunctionName = "RetTypeComp",
    Root = new CompositionAST
    {
        FunctionName = "RetType",
        ChildrenInOrder = new CompositionAST[] { },
    }
};

Console.WriteLine($"\nFunction registered: {controller.RegisterFunction(foo)}");
Console.WriteLine($"\nFunction registered: {controller.RegisterFunction(bar)}");
Console.WriteLine($"\nFunction registered: {controller.RegisterFunction(complex)}");
Console.WriteLine($"\nFunction registered: {controller.RegisterFunction(simple)}");
Console.WriteLine($"\nFunction composed: {controller.RegisterComposition(composedFunc)}");
Console.WriteLine($"\nFunction composed: {controller.RegisterComposition(composedFunc2)}");
Console.WriteLine($"\nFunction composed: {controller.RegisterComposition(simpleComp)}");

// Execute function through the mediator grain to test end-to-end functionality

await controller.ExecuteFunction(new FunctionExecutionRequest
{
    FunctionName = "AddThenIncrement",
    Parameters = new object[] { 10L, 20L }
});

await controller.ExecuteFunction(new FunctionExecutionRequest
{
    FunctionName = "BranchedComposition",
    Parameters = new object[] { 0L }
});

await controller.ExecuteFunction(new FunctionExecutionRequest
{
    FunctionName = "RetTypeComp",
    Parameters = new object[] { }
});

Console.WriteLine("Finished with dev tests");

Thread.Sleep(4000);

*/

// Run transaction client (using new RedisKVS)
var transactionClient = new TransactionClient();
await transactionClient.RunClient();
return;
