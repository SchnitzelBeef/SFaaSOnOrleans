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

// Run transaction client (using new RedisKVS)
var transactionClient = new TransactionClient();
await transactionClient.RunClient();
return;
