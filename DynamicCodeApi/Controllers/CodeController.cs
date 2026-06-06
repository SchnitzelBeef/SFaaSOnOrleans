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
    [HttpPost("register")]
    public IActionResult RegisterFunction([FromBody] CodeRegistrationRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.FunctionName) || string.IsNullOrWhiteSpace(request.Code))
            return BadRequest("Function name and code must be provided.");

        if(this.kvs.PutString(request.FunctionName, request.Code))
            return Ok($"Function '{request.FunctionName}' registered successfully.");
        return BadRequest($"Error registering function: {request.FunctionName}");
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

        // remove the last ';' to allow code composition
        string firstCode = this.kvs.GetString(request.CompositionFirstFunctionName); //.TrimEnd(';'); 
        string secondCode = this.kvs.GetString(request.CompositionSecondFunctionName); //.TrimEnd(';');

        Console.WriteLine($"First function code: {firstCode}");
        Console.WriteLine($"Second function code: {secondCode}");

        string composedCode = $@"
            System.Func<object[], object> first = args => {{
                {firstCode}
            }};
            System.Func<object[], object> second = args => {{
                {secondCode}
            }};
            return second(new object[] {{ first(args) }});";

        Console.WriteLine($"Composed function code: {composedCode}");

        if(this.kvs.PutString(request.FunctionName, composedCode))
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

