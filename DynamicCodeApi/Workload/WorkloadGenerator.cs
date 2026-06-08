using Controller;
using DynamicCodeApi;
using Infra.EcommerceFunctions;
using Infra.EcommerceStates;
using MathNet.Numerics.Distributions;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace Workload;

internal class WorkloadGenerator
{
    private readonly int numCustomerActor;
    private readonly int numProductActor;
    private IClusterClient client;
    private bool isClientConnected = false;

    private IDiscreteDistribution customerDistribution;       // which customer send the request
    private IDiscreteDistribution productDistribution;        // which product to buy
    private IDiscreteDistribution productQtyDistribution;     // the number of items available for each product
    private IDiscreteDistribution productPriceDistribution;   // the price of items
    private IDiscreteDistribution customerBalanceDistribution;// the customer balance
    private IDiscreteDistribution customerQtyDistribution;    // max qty a customer can buy for a product

    // Used to issue requests to RedisKSV
    // Maybe a bit overkill since we can call the functions directly without HTTP wrappers
    private CodeController controller;

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

    private async void InitiateClient()
    {
        this.client = await OrleansClientManager.GetClient();
        isClientConnected = true;
    }

    public async Task InitAllActorFunctions()
    {
        // Init all actor functions in the Redis KVS:

        // ------ Customer functions ------

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "ProcessCheckout",
            Code = CustomerFunctions.GetProcessCheckoutFunction()
        });

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "ProcessOutcome",
            Code = CustomerFunctions.GetProcessOutcomeFunction()
        });

        // Not really necessary, since we can get the balance directly in the RedisKVS
        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "GetBalance",
            Code = CustomerFunctions.GetGetBalanceFunction()
        });


        // ------ Products functions ------
        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "ProcessInventoryRequest",
            Code = ProductFunctions.GetProcessInventoryRequestFunction()
        });

        // Not really necessary, since we can get the price directly in the RedisKVS
        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "GetPrice",
            Code = ProductFunctions.GetGetPriceFunction()
        });

        // Not really necessary, since we can get the inventory quantity directly in the RedisKVS
        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "GetInventory",
            Code = ProductFunctions.GetGetInventoryFunction()
        });


        // ------ Analytics functions ------

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "GetUpdateAsync",
            Code = AnalyticsFunctions.GetGetUpdateAsyncFunction()
        });

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "Top10",
            Code = AnalyticsFunctions.GetTop10Function()
        });

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "CustomerOutcomeProcessedCount",
            Code = AnalyticsFunctions.GetCustomerOutcomeProcessedCountFunction()
        });

        this.controller.RegisterFunction(new CodeRegistrationRequest
        {
            FunctionName = "GetSumOfAllBalance",
            Code = AnalyticsFunctions.GetGetSumOfAllBalanceFunction()
        });


        // ------ Workflows ------
        // this.controller.RegisterComposition(new FunctionCompositionRequest
        // {
        //     FunctionName = "NewCheckoutOrder",
        //     CompositionFunctionNames = new string[] { "ProcessCheckout", "ProcessInventoryRequest" },
        // });

    }

    // Unpacker function
    private T FunctionExecutionUnpacker<T>(IActionResult result, T bad_result)
    {
        if (result is OkObjectResult ok)
        {
            return (T)ok.Value;
        }
        else if (result is BadRequestObjectResult bad)
        {
            Console.WriteLine($"Error: Got bad result '{bad}' when unpacking HTTP function execution result");
        }
        return bad_result;
    }

    public async Task InitAllActors()
    {
        // Init all actors with an initial state in the Redis KVS:

        // ------ Customer ------

        for (int i = 0; i < numCustomerActor; i++)
        {
            ObejctRegistrationRequest customerState = new ObejctRegistrationRequest
            {
                Key = $"Customer-{i}",
                Object = new CustomerState { Balance = customerBalanceDistribution.Sample() }
            };
            this.controller.RegisterKeyObject(customerState);
        }


        // ------ Products ------

        for (int i = 0; i < numProductActor; i++)
        {
            ObejctRegistrationRequest productState = new ObejctRegistrationRequest
            {
                Key = $"Product-{i}",
                Object = new ProductState { Price = productPriceDistribution.Sample(), Quantity = productQtyDistribution.Sample() }
            };
            this.controller.RegisterKeyObject(productState);
        }



        // ------ Analytics ------

        ObejctRegistrationRequest analyticsState = new ObejctRegistrationRequest
        {
            Key = $"Analytics-0",
            Object = new AnalyticsState { Query = new Dictionary<long, double>() }
        };
        this.controller.RegisterKeyObject(analyticsState);


        // EXAMPLES FOR TESTS
        // Example of processing checkout
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "ProcessCheckout",
        //     Parameters = new object[] {0L, new Checkout(0, 10, 2)}
        // });

        // Thread.Sleep(3000);

        // // Example of processing inventory
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "ProcessInventoryRequest",
        //     Parameters = new object[] {0L, new Inventory(1, 1, 1)}
        // });

        // Thread.Sleep(3000);

        // Example of processing NewCheckoutOrder
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "NewCheckoutOrder",
        //     Parameters = new object[] {0L, new Checkout(0, 10, 2)}
        // });
    }

    public async Task<Tuple<List<long>, bool>> GetAllInventory()
    {
        var tasks = new List<Task<IActionResult>>();
        for (long i = 0; i < numProductActor; i++)
        {
            // Obs use of TestFunction to bypass mediator grain
            // Can use when we do not want to initiate workflow, where the result is harder to get directly
            tasks.Add(
                this.controller.TestFunction(new FunctionExecutionRequest
                {
                    FunctionName = "GetInventory",
                    Parameters = new object[] { i }
                }
                )
            );
        }
        await Task.WhenAll(tasks);

        var hasEverGotNegativeInventory = false;
        var inventory = new List<long>();
        foreach (var task in tasks)
        {
            var res = FunctionExecutionUnpacker<int>(task.Result, 0); // obs standard value for bad result
            inventory.Add(res);
            if (res < 0) hasEverGotNegativeInventory = true;
        }
        return new Tuple<List<long>, bool>(inventory, hasEverGotNegativeInventory);
    }

    // OBS: Should be implemented. Waiting for composition to work
    public async Task NewCheckOutOrder()
    {
        var customerID = customerDistribution.Sample();
        var productID = productDistribution.Sample();
        var qty = customerQtyDistribution.Sample();


        // throw new NotImplementedException();

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
        
        var res = await this.controller.TestFunction(new FunctionExecutionRequest
            {
                FunctionName = "Top10",
                Parameters = new object[] { }
            }
        );

        var unpacked_res = FunctionExecutionUnpacker<List<KeyValuePair<long, double>>>(res, null);

        if (unpacked_res == null)
        {
            return "Function execution error in controller when fetching Top10 from Analytics-0";
        }

        StringBuilder sb = new StringBuilder();
        foreach (KeyValuePair<long, double> kv in unpacked_res)
        {
            sb.Append(kv.Key);
            sb.Append(" : ");
            sb.Append(kv.Value);
            sb.AppendLine();
        }
        return sb.ToString();
    }

}

