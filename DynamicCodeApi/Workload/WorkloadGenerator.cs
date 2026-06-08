using Controller;
using DynamicCodeApi;
using Infra.EcommerceFunctions;
using Infra.EcommerceStates;
using Infra.EventSchema;
using MathNet.Numerics.Distributions;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace Workload;

internal class WorkloadGenerator
{
    private readonly int numCustomerActor;
    private readonly int numProductActor;
    private OrleansClientManager clientManager;
    private IClusterClient client;
    private bool isClientConnected = false;

    private IDiscreteDistribution customerDistribution;       // which customer send the request
    private IDiscreteDistribution productDistribution;        // which product to buy
    private IDiscreteDistribution productQtyDistribution;     // the number of items available for each product
    private IDiscreteDistribution productPriceDistribution;   // the price of items
    private IDiscreteDistribution customerBalanceDistribution;// the customer balance
    private IDiscreteDistribution customerQtyDistribution;    // max qty a customer can buy for a product
    private IDiscreteDistribution isCheckoutElseTop10;        // checkout = 0, top10 = 1

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
        isCheckoutElseTop10 = new DiscreteUniform(0, 1, new Random());

        // wait until the client is created and connected
        InitiateClient();
        while (isClientConnected == false) Thread.Sleep(TimeSpan.FromMilliseconds(100));
    }

    private async void InitiateClient()
    {
        this.clientManager = new OrleansClientManager();
        this.client = await this.clientManager.StartClient();
        isClientConnected = true;
    }

    public async Task StopClient()
    {
        await this.clientManager.StopClient();
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
                Object = new CustomerState
                {
                    Balance = customerBalanceDistribution.Sample()
                }
            };
            this.controller.RegisterKeyObject(customerState);
        }


        // ------ Products ------

        for (int i = 0; i < numProductActor; i++)
        {
            ObejctRegistrationRequest productState = new ObejctRegistrationRequest
            {
                Key = $"Product-{i}",
                Object = new ProductState
                {
                    Price = productPriceDistribution.Sample(),
                    Quantity = productQtyDistribution.Sample()
                }
            };
            this.controller.RegisterKeyObject(productState);
        }



        // ------ Analytics ------

        ObejctRegistrationRequest analyticsState = new ObejctRegistrationRequest
        {
            Key = $"Analytics-0",
            Object = new AnalyticsState
            {
                Query = new Dictionary<long, double>(),
                DebugQuery = new Dictionary<long, int>()
            }
        };
        this.controller.RegisterKeyObject(analyticsState);


        // EXAMPLES FOR TESTING

        // Example of processing checkout
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "ProcessCheckout",
        //     Parameters = new object[] {0L, new Checkout(0, 10, 2)}
        // });

        // Example of processing inventory
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "ProcessInventoryRequest",
        //     Parameters = new object[] {0L, new Inventory(1, 1, 1)}
        // });

        // Example of processing outcome
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "UpdateAsync",
        //     Parameters = new object[] {new Outcome(0, 0, 10, Status.OK)}
        // });

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
            var res = FunctionExecutionUnpacker<int>(task.Result, 0); // obs standard value for bad result, should probably change
            inventory.Add(res);
            if (res < 0) hasEverGotNegativeInventory = true;
        }
        return new Tuple<List<long>, bool>(inventory, hasEverGotNegativeInventory);
    }

    public async Task<Tuple<List<double>, bool>> GetAllBalance()
    {
        var tasks = new List<Task<IActionResult>>();
        for (long i = 0; i < numCustomerActor; i++)
        {
            tasks.Add(
                this.controller.TestFunction(new FunctionExecutionRequest
                {
                    FunctionName = "GetBalance",
                    Parameters = new object[] { i }
                }
                )
            );
        }
        await Task.WhenAll(tasks);

        var hasEverGotNegativeBalance = false;
        var balances = new List<double>();
        foreach (var task in tasks)
        {
            // obs standard value for bad result, should probably change
            var res = FunctionExecutionUnpacker<double>(task.Result, 0); 
            balances.Add(res);
            if (res < 0) hasEverGotNegativeBalance = true;
        }
        return new Tuple<List<double>, bool>(balances, hasEverGotNegativeBalance);
    }

    public async Task NewOrder()
    {
        var isCheckout = isCheckoutElseTop10.Sample() == 0;
        if (isCheckout)
        {
            var customerId = customerDistribution.Sample();
            this.NewCheckOutOrder(customerId);
        }
        else
        {
            var wrapped_res = await this.controller.TestFunction(new FunctionExecutionRequest
                {
                    FunctionName = "Top10",
                    Parameters = new object[] { }
                }
            );

            // OBS, no handling if null is returned
            var _ = FunctionExecutionUnpacker<List<KeyValuePair<long, double>>>(wrapped_res, null);
        }
    }


    public async Task NewCheckOutOrder(long customerID)
    {
        long productID = productDistribution.Sample();
        var qty = customerQtyDistribution.Sample();

        var wrapped_price = await this.controller.TestFunction(new FunctionExecutionRequest
        {
            FunctionName = "GetPrice",
            Parameters = new object[] { productID }
        });


        // OBS, use of -1 for "bad" value
        // Should probably do error handling, but error should not be able to occur (at least, with very high prob. due to HTTP) in local test-setup
        var price = FunctionExecutionUnpacker<double>(wrapped_price, -1); 

        var checkout = new Checkout(productID, price, qty);

        // Uncomment when 'NewCheckoutOrder' workflow is implemented
        // await this.controller.ExecuteFunction(new FunctionExecutionRequest
        // {
        //     FunctionName = "NewCheckoutOrder",
        //     Parameters = new object[] { customerID, checkout }
        // });
    }

    public async Task<int> GetCustomerProcessedCount(long customerId)
    {
        var wrapped_res = await this.controller.TestFunction(new FunctionExecutionRequest
            {
                FunctionName = "CustomerOutcomeProcessedCount",
                Parameters = new object[] { customerId }
            }
        );

        // OBS, no handling if -1 is returned
        return FunctionExecutionUnpacker<int>(wrapped_res, -1);
    }

    public async Task<string> GetTopTen()
    {
        
        var wrapped_res = await this.controller.TestFunction(new FunctionExecutionRequest
            {
                FunctionName = "Top10",
                Parameters = new object[] { }
            }
        );

        var res = FunctionExecutionUnpacker<List<KeyValuePair<long, double>>>(wrapped_res, null);

        if (res == null)
        {
            return "Function execution error in controller when fetching Top10 from Analytics-0";
        }

        StringBuilder sb = new StringBuilder();
        foreach (KeyValuePair<long, double> kv in res)
        {
            sb.Append(kv.Key);
            sb.Append(" : ");
            sb.Append(kv.Value);
            sb.AppendLine();
        }
        return sb.ToString();
    }

}

