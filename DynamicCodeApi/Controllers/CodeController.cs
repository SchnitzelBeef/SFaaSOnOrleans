using DynamicCodeApi;
using Infra.Interfaces;
using Infra.Service;
using Microsoft.AspNetCore.Mvc;
using Infra.Kafka;

namespace Controller
{
public class CodeRegistrationRequest
{
    public string FunctionName { get; set; }
    public string Code { get; set; }
}

public class ValueRegistrationRequest
{
    public string Key { get; set; }
    public string Value { get; set; }
}

public class ObejctRegistrationRequest
{
    public string Key { get; set; }
    public Object Object { get; set; }
}

public class KeyValueGetRequest
{
    public string Key { get; set; }
}


public class FunctionExecutionRequest
{
    public string FunctionName { get; set; }
    public object[] Parameters { get; set; }
}

public class FunctionCompositionRequest
{
    public string FunctionName { get; set; }
    public string CompositionFirstFunctionName { get; set; }
    public string CompositionSecondFunctionName { get; set; }
}

[ApiController]
public class CodeController : ControllerBase
{
    private readonly IKeyValueStore kvs;
    private readonly IClusterClient client;

    public CodeController(IKeyValueStore kvs)
    {
        this.kvs = kvs;
        this.client = OrleansClientManager.GetClient().Result;
    }

    // Register function
    [HttpPost("registerFun")]
    public IActionResult RegisterFunction([FromBody] CodeRegistrationRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.FunctionName) || string.IsNullOrWhiteSpace(request.Code))
            return BadRequest("Function name and code must be provided.");

        if(this.kvs.PutString(request.FunctionName, request.Code))
            Console.WriteLine($"Function '{request.FunctionName}' registered := {request.Code}");
            return Ok($"Function '{request.FunctionName}' registered successfully.");
        return BadRequest($"Error registering function: {request.FunctionName}");
    }

    // Register key-value, could also use 'RegisterFunction' instead
    [HttpPost("registerVal")]
    public IActionResult RegisterKeyValue([FromBody] ValueRegistrationRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Key) || string.IsNullOrWhiteSpace(request.Value))
            return BadRequest("Key and value must be provided.");

        if(this.kvs.PutString(request.Key, request.Value))
            return Ok($"Key '{request.Key}' registered successfully.");
        return BadRequest($"Error registering function: {request.Key}");
    }


    // Register key-value (object), could also most likely use 'RegisterFunction' instead
    [HttpPost("registerObj")]
    public IActionResult RegisterKeyObject([FromBody] ObejctRegistrationRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Key))
            return BadRequest("Key must be provided.");

        if(this.kvs.Put(request.Key, request.Object))
            return Ok($"Key '{request.Key}' registered successfully.");
        return BadRequest($"Error registering function: {request.Key}");
    }


    // compose function (added)
    [HttpPost("compose")]
    public IActionResult RegisterComposition([FromBody] FunctionCompositionRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.FunctionName) ||
            string.IsNullOrWhiteSpace(request.CompositionFirstFunctionName) ||
            string.IsNullOrWhiteSpace(request.CompositionSecondFunctionName))
            return BadRequest("All function names must be provided.");

        if (this.kvs.GetString(request.CompositionFirstFunctionName) == null){
            return NotFound($"Function '{request.CompositionFirstFunctionName}' not found.");
        }

        if (this.kvs.GetString(request.CompositionSecondFunctionName) == null){
            return NotFound($"Function '{request.CompositionSecondFunctionName}' not found.");
        }

        string firstCode = this.kvs.GetString(request.CompositionFirstFunctionName);
        string secondCode = this.kvs.GetString(request.CompositionSecondFunctionName);

        string composedCode = $@"
            System.Func<object[], object> first = args => {{
                {firstCode}
            }};
            
            var result = new object[] {{ first(args) }};
            return (object)new Infra.Kafka.Event(""{request.CompositionSecondFunctionName}"", result);";

        // string composedCode = $@"
        //     System.Func<object[], object> first = args => {{
        //         {firstCode}
        //     }};
        //     System.Func<object[], object> second = args => {{
        //         {secondCode}
        //     }};
        //     return second(new object[] {{ first(args) }});";


        if(this.kvs.PutString(request.FunctionName, composedCode))
            Console.WriteLine($"Composed function '{request.FunctionName}' registered := {composedCode}");
            return Ok($"Function '{request.FunctionName}' registered successfully.");
        return BadRequest($"Error registering function: {request.FunctionName}");
    }

    // Execute function
    [HttpPost("execute")]
    public async Task<IActionResult> ExecuteFunction([FromBody] FunctionExecutionRequest request)
    {
        if (this.kvs.GetString(request.FunctionName) == null){
            return NotFound($"Function '{request.FunctionName}' not found.");
        }
        try
        {
            var result = await this.Dispatch(request.FunctionName, request.Parameters);
            return Ok(result);
        }
        catch (Exception ex)
        {
            return BadRequest($"Error executing function: {ex}");
        }
    }

    // Test get value from KVS
    [HttpPost("get")]
    public async Task<IActionResult> Get([FromBody] KeyValueGetRequest request)
    {
        var res = this.kvs.GetString(request.Key);
        if (res == null){
            return NotFound($"Key '{request.Key}' not found.");
        }
        return Ok(res);
    }

    // Test function
    [HttpPost("test")]
    public async Task<IActionResult> TestFunction([FromBody] FunctionExecutionRequest request)
    {
        if (this.kvs.GetString(request.FunctionName) == null){
            return NotFound($"Function '{request.FunctionName}' not found.");
        }
        try
        {
            // Example below should only be used for testing purposes
            var worker = this.client.GetGrain<IExecutorGrain>(0);
            return Ok(await worker.Execute(request.FunctionName, request.Parameters));
        }
        catch (Exception ex)
        {
            return BadRequest($"Error executing function: {ex}");
        }
    }

    // Dispatch the function workflow request for execution (added)
    private Task<object> Dispatch(string functionName, object[] parameters)
    {
        // Pick a well-defind MediatorGrain and call StartWorkflow
        
        var mediatorGrain = this.client.GetGrain<IMediatorGrain>("mediator"); //obs
        mediatorGrain.StartWorkflow(functionName, parameters);
        return Task.FromResult((object)null);   
    }

    // Clear Redis
    [HttpPost("resetKVS")]
    public IActionResult ResetKVS()
    {
        this.kvs.Reset();
        return Ok();
    }

}
    
}

