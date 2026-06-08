using Infra;
using Infra.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Serialization;

using var host = new HostBuilder()
    .UseOrleans(builder =>
    {
        builder
            .UseLocalhostClustering()
            .Configure<ClusterOptions>(options =>
            {
                options.ClusterId = Constants.ClusterId;
                options.ServiceId = Constants.ServiceId;
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Error);
            })
            .Services.AddSerializer(ser =>
            {
                ser.AddNewtonsoftJsonSerializer(isSupported: type => type.Namespace.StartsWith("Infra"));
            })
            .AddSingleton<IKeyValueStore, RedisKVS>();
        ;
    })
    .Build();


await host.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("    The Orleans silo started. Press Enter to terminate...    ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
await host.StopAsync();

// var result = controller.RegisterFunction(foo);
// if (result is OkObjectResult ok)
//     Console.WriteLine($"Success: {ok.Value}");
// else if (result is BadRequestObjectResult bad)
//     Console.WriteLine($"Error: {bad.Value}");
