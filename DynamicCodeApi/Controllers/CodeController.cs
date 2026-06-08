using DynamicCodeApi;
using Infra.Interfaces;
using Infra.Kafka;
using Infra.Service;
using Microsoft.AspNetCore.Mvc;

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
        public string[] CompositionFunctionNames { get; set; }
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

            if (this.kvs.PutString(request.FunctionName, request.Code))
                Console.WriteLine($"Function '{request.FunctionName}' registered in RedisKVS := {request.Code}");
            return Ok($"Function '{request.FunctionName}' registered successfully.");
        }

        // Register key-value, could also use 'RegisterFunction' instead
        [HttpPost("registerVal")]
        public IActionResult RegisterKeyValue([FromBody] ValueRegistrationRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Key) || string.IsNullOrWhiteSpace(request.Value))
                return BadRequest("Key and value must be provided.");

            if (this.kvs.PutString(request.Key, request.Value))
                return Ok($"Key '{request.Key}' registered successfully.");
            return BadRequest($"Error registering function: {request.Key}");
        }


        // Register key-value (object), could also most likely use 'RegisterFunction' instead
        [HttpPost("registerObj")]
        public IActionResult RegisterKeyObject([FromBody] ObejctRegistrationRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Key))
                return BadRequest("Key must be provided.");

            if (this.kvs.Put(request.Key, request.Object))
                return Ok($"Key '{request.Key}' registered successfully.");
            return BadRequest($"Error registering function: {request.Key}");
        }


        // Compose function
        [HttpPost("compose")]
        public IActionResult RegisterComposition([FromBody] FunctionCompositionRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.FunctionName))
                return BadRequest("Function name must be provided.");

            foreach (var name in request.CompositionFunctionNames)
            {
                if (string.IsNullOrWhiteSpace(name))
                    return BadRequest("Function composition name must be provided.");

                if (this.kvs.GetString(name) == null)
                    return NotFound($"Function '{name}' not found.");
            }

            var namePrefix = request.FunctionName + "Workflow";
            for (var i = 0; i < request.CompositionFunctionNames.Length; i++)
            {
                var name = request.CompositionFunctionNames[i];
                var code = this.kvs.GetString(name);

                // First function is named after the workflow function
                var functionName = request.FunctionName;
                if (i != 0)
                {
                    functionName = $"{namePrefix}{i}{name}";
                }

                // Last function is null
                var nextNameInCode = "null";
                if (i != request.CompositionFunctionNames.Length - 1)
                {
                    var nextName = request.CompositionFunctionNames[i + 1];
                    nextNameInCode = $"\"{namePrefix}{i + 1}{nextName}\"";
                }

                // Every other function is named based uniquely on workflow + index + name

                // Create a new intermediate function for this workflow step
                // It returns both its result and the name of the next function
                RegisterFunction(new CodeRegistrationRequest
                {
                    FunctionName = functionName,
                    Code = $@"
                        System.Func<object[], object> __inner__ = args => {{ {code} }};
                        return new System.Tuple<string, object>({nextNameInCode}, __inner__(args));",
                });
            }

            return Ok($"Succesfully created workflow {request.FunctionName}");
        }

        // Execute function
        [HttpPost("execute")]
        public async Task<IActionResult> ExecuteFunction([FromBody] FunctionExecutionRequest request)
        {
            if (this.kvs.GetString(request.FunctionName) == null)
            {
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
            if (res == null)
            {
                return NotFound($"Key '{request.Key}' not found.");
            }
            return Ok(res);
        }

        // Test function
        [HttpPost("test")]
        public async Task<IActionResult> TestFunction([FromBody] FunctionExecutionRequest request)
        {
            if (this.kvs.GetString(request.FunctionName) == null)
            {
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

            // Could also be made to return an actual result
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
