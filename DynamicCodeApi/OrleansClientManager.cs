using Infra;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Configuration;
using Orleans.Serialization;

namespace DynamicCodeApi;

public class OrleansClientManager
{

    private IHost host;

    public OrleansClientManager()
        {
        this.host = new HostBuilder()
            .UseOrleansClient(clientBuilder =>
            {
                clientBuilder.UseLocalhostClustering();
                clientBuilder.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterId;
                    options.ServiceId = Constants.ServiceId;    
                });
            })
            //.ConfigureLogging(loggingBuilder => loggingBuilder.AddConsole())
            .ConfigureServices(f => f.AddSerializer(ser =>
            {
                ser.AddNewtonsoftJsonSerializer(isSupported: type => type.Namespace.StartsWith("Infra"));
            }))
            .Build();

        // await client.StartAsync();

        // return client.Services.GetService<IClusterClient>();
    }
    

    public async Task StopClient()
    {
        await this.host.StopAsync();
    }

    public async Task<IClusterClient> StartClient()
    {
        await this.host.StartAsync();
        return this.host.Services.GetService<IClusterClient>();
    }   
}
