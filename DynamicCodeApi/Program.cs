using Infra.Interfaces;
using DynamicCodeApi.Workload;
using Infra.Interfaces;
using Infra.Service;
using DynamicCodeApi;
using Infra.Kafka;
using Infra;
using Controller;

var sharedClient = OrleansClientManager.GetClient().Result;

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

// Test-setup, should not be part of final handin
// Just used to test that we can submit functions to the Redis KVS and execute them through the mediator grain
Console.WriteLine("Initializing test...");

var redisKVS = new RedisKVS(null); // Null for logger
var controller = new CodeController(redisKVS);

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

var composedFunc = new FunctionCompositionRequest
{
    FunctionName = "AddThenIncrement",
    CompositionFirstFunctionName = "AddNumbers",
    CompositionSecondFunctionName = "Increment"
};

Console.WriteLine($"Function registered: {controller.RegisterFunction(foo)}");
Console.WriteLine($"Function registered: {controller.RegisterFunction(bar)}");
Console.WriteLine($"Function composed: {controller.RegisterComposition(composedFunc)}");

// Execute function through the mediator grain to test end-to-end functionality

var mediatorGrain = sharedClient.GetGrain<IMediatorGrain>("mediator"); //obs, temporary setup
await mediatorGrain.Init(Constants.CheckoutNamespace, Constants.CheckoutTopicGroup, 0, -1);

await controller.ExecuteFunction(new FunctionExecutionRequest
{
    FunctionName = "AddThenIncrement",
    Parameters = new object[] { 10L, 20L }
});


Thread.Sleep(2000);


// Run transaction client (using new RedisKVS)
var transactionClient = new TransactionClient();
await transactionClient.RunClient();
return;
