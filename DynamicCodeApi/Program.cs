using DynamicCodeApi;
using DynamicCodeApi.Workload;
using Infra;
using Infra.Interfaces;
using Infra.Kafka;
using Infra.Service;

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

for (int i = 0; i < Constants.NumMediatorActors; i++)
{
    await sharedClient.GetGrain<IMediatorGrain>(i).Init();
}
for (int i = 0; i < Constants.NumExecutorActors; i++)
{
    await sharedClient.GetGrain<IExecutorGrain>(i).Init();
}

// Run transaction client (using new RedisKVS)
var transactionClient = new TransactionClient();
await transactionClient.RunClient();
return;
