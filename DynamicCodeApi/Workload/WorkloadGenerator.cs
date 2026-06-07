using System.Text;
using Controller;
using DynamicCodeApi;
using MathNet.Numerics.Distributions;
using Infra.EcommerceStates;
using Infra.EcommerceFunctions;
using Infra.EventSchema;

namespace Workload;

internal class WorkloadGenerator
{
    readonly int numCustomerActor;
    readonly int numProductActor;
    IClusterClient client;
    bool isClientConnected = false;
      
    IDiscreteDistribution customerDistribution;       // which customer send the request
    IDiscreteDistribution productDistribution;        // which product to buy
    IDiscreteDistribution productQtyDistribution;     // the number of items available for each product
    IDiscreteDistribution productPriceDistribution;   // the price of items
    IDiscreteDistribution customerBalanceDistribution;// the customer balance
    IDiscreteDistribution customerQtyDistribution;    // max qty a customer can buy for a product

    // Used to issue requests to RedisKSV
    // Maybe a bit overkill since we can call the functions directly without HTTP wrappers
    CodeController controller;

    public WorkloadGenerator(int numCustomerActor, int numProductActor, CodeController controller)
    {
         
        this.numCustomerActor = numCustomerActor;
        this.numProductActor = numProductActor;
        this.controller = controller;
        // it will generate samples within range [a, b]
        customerDistribution = new DiscreteUniform(0, this.numCustomerActor - 1, new Random());
        productDistribution = new DiscreteUniform(0, this.numProductActor - 1, new Random());
        productQtyDistribution = new DiscreteUniform(1, 100, new Random());
        productPriceDistribution = new DiscreteUniform(1, 1000, new Random());
        customerBalanceDistribution = new DiscreteUniform(1, 10000, new Random());
        customerQtyDistribution = new DiscreteUniform(1, 10, new Random());

        // wait until the client is created and connected
        InitiateClient();
        while (isClientConnected == false) Thread.Sleep(TimeSpan.FromMilliseconds(100));
    }

    async void InitiateClient()
    {
        this.client = await OrleansClientManager.GetClient();
        isClientConnected = true;
    }

    public async Task InitAllActors()
    {
        // We must init all actors with an initial state in the Redis KVS:

        // ------ Customer ------

        for (int i = 0; i < numCustomerActor; i++)
        {
            ObejctRegistrationRequest customerState = new ObejctRegistrationRequest
            {
                Key = $"Customer-{i}",
                Object = new CustomerState{Balance = customerBalanceDistribution.Sample()}
            };
            this.controller.RegisterKeyObject(customerState);
        }

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "ProcessCheckout",
            Code = CustomerFunctions.GetProcessCheckout()

        });

        // Example of processing checkout
        await this.controller.ExecuteFunction(new FunctionExecutionRequest
        {
            FunctionName = "ProcessCheckout",
            Parameters = new object[] {0L, new Checkout(0, 10, 2)}
        });

        // // ------ Products ------

        // for (int i = 0; i < numProductActor; i++)
        // {
        //     KeyValueRequest productState = new KeyValueRequest
        //     {
        //         Key = $"Product-{i}",
        //         Value = $"{new ProductState(productPriceDistribution.Sample(), productQtyDistribution.Sample())};"
        //     };
        //     this.controller.RegisterKeyValue(productState);
        // }

        // // ------ Analytics ------

        // KeyValueRequest analyticsState = new KeyValueRequest
        // {
        //     Key = $"Analytics-0",
        //     Value = $"{new AnalyticsState()};"
        // };
        // this.controller.RegisterKeyValue(analyticsState);

        // Code commented out in handout code
        // throw new NotImplementedException();
        /*
        var analyticsActor = client.GetGrain<IAnalyticsActor>(0);
        await analyticsActor.Init();

        var tasks = new List<Task>();
        for (int i = 0; i < numCustomerActor; i++)
        {
            var customerActor = client.GetGrain<ICustomerActor>(i);
            tasks.Add(customerActor.Init(customerBalanceDistribution.Sample()));  
        }

        for (int i = 0; i < numProductActor; i++)
        {
            var productActor = client.GetGrain<IProductActor>(i);
            tasks.Add(productActor.Init(productPriceDistribution.Sample(), productQtyDistribution.Sample()));
        }
               
        await Task.WhenAll(tasks);
        */
    }

    public async Task<Tuple<List<long>, bool>> GetAllInventory()
    {
        var tasks = new List<Task<int>>();
        for (int i = 0; i < numProductActor; i++)
        {
            // this.controller.Get($"Product_{{{i}}}") // obs

            /*
            var productActor = client.GetGrain<IProductActor>(i);
            tasks.Add(productActor.GetInventory());
            */
           
        }
        await Task.WhenAll(tasks);

        var hasEverGotNegativeInventory = false;
        var inventory = new List<long>();
        foreach (var task in tasks)
        {
            inventory.Add(task.Result);
            if (task.Result < 0) hasEverGotNegativeInventory = true;
        }
        return new Tuple<List<long>, bool>(inventory, hasEverGotNegativeInventory);
    }

    public async Task NewCheckOutOrder()
    {
        var customerID = customerDistribution.Sample();
        var productID = productDistribution.Sample();
        var qty = customerQtyDistribution.Sample();
        throw new NotImplementedException();
        /*
        var price = await client.GetGrain<IProductActor>(productID).GetPrice();

        IStreamProvider streamProvider = client.GetStreamProvider(Constants.DefaultStreamProvider);

        IAsyncStream<Checkout> checkoutStream = streamProvider.GetStream<Checkout>( Constants.CheckoutNamespace, customerID.ToString() );
        await checkoutStream.OnNextAsync(new Checkout(productID, price, qty));
        return;
        */
    }

    public async Task<string> GetTopTen()
    {
        List<KeyValuePair<long, double>> res = null;
        // res = await client.GetGrain<IAnalyticsActor>(0).Top10();
        StringBuilder sb = new StringBuilder();
        foreach(KeyValuePair<long, double> kv in res)
        {
            sb.Append (kv.Key);
            sb.Append(" : ");
            sb.Append(kv.Value);
            sb.AppendLine();
        }
        return sb.ToString();
    }

}

