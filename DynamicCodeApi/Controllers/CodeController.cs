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
        public CompositionAST Root { get; set; }
    }

    public class CompositionAST
    {
        public string FunctionName { get; set; }
        public CompositionAST[] ChildrenInOrder { get; set; }
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
                return BadRequest("Function name is missing.");

            if (ValidateCompositionTree(request.Root))
            {
                return BadRequest("Malformed composition tree. Missing proper functions names.");
            }

            HandleCompositionTree(request.Root, 0, 0, request.FunctionName);

            return Ok($"Succesfully created workflow {request.Root.FunctionName}");
        }

        private bool ValidateCompositionTree(CompositionAST node)
        {
            if (node == null)
            {
                // Allow empty children
                return false;
            }

            if (string.IsNullOrWhiteSpace(node.FunctionName))
                return true;

            if (this.kvs.GetString(node.FunctionName) == null)
                return true;

            foreach (var child in node.ChildrenInOrder)
            {
                if (ValidateCompositionTree(child))
                {
                    return true;
                }
            }
            return false;
        }

        private void HandleCompositionTree(CompositionAST node, int depth, int branch, string rootName)
        {
            if (node == null)
            {
                // Allow empty children
                return;
            }

            for (var i = 0; i < node.ChildrenInOrder.Length; i++)
            {
                HandleCompositionTree(node.ChildrenInOrder[i], depth + 1, i, rootName);
            }

            var namePrefix = "Workflow" + rootName;

            var code = this.kvs.GetString(node.FunctionName);

            // First function is named after the workflow function
            string functionName;
            if (depth == 0)
                functionName = rootName;
            else
                functionName = $"{namePrefix}_{depth}_{branch}_{node.FunctionName}";

            // Every other function is named based uniquely on workflow + index + name
            var childNames = string.Join(",", node.ChildrenInOrder.Select((n, i) =>
            {
                if (n != null)
                    return $"\"{namePrefix}_{depth + 1}_{i}_{n.FunctionName}\"";
                else
                    return "null";
            }));

            // Create a new intermediate function for this workflow step
            // It returns both its result and the name of the next function
            RegisterFunction(new CodeRegistrationRequest
            {
                FunctionName = functionName,
                Code = $@"
                    System.Func<object[], object> __inner__{functionName}__ = args => {{ {code}; return null; }};
                    var result = __inner__{functionName}__(args);
                    
                    string[] childMap = new string[] {{{childNames}}};
                    
                    if (result is System.Tuple<int, object>) {{
                        var tuple = (System.Tuple<int, object>)result;
                        
                        if (childMap.Length == 0) {{
                            return new System.Tuple<string, object>(null, tuple.Item2);
                        }}

                        int index = tuple.Item1;
                        // Ignore duplicate events:
                        if (index == -1) {{
                            return new System.Tuple<string, object>(null, $""Duplicate event detected, cancelling workflow in: {{tuple.Item2}}"");
                        }}
                        if (index < -1) {{
                            return new System.Tuple<string, object>(null, $""ERROR: Workflow failed: {{tuple.Item2}}"");
                        }}
                        if (index >= childMap.Length) {{
                            return new System.Tuple<string, object>(null, ""ERROR: Invalid workflow child mapping. Index out of bounds."");
                        }}
                        string nextFunction = childMap[index];
                        return new System.Tuple<string, object>(nextFunction, tuple.Item2);
                    }} else {{
                        if (childMap.Length == 0) {{
                            return new System.Tuple<string, object>(null, result);
                        }}
                        if (childMap.Length != 1) {{
                            return new System.Tuple<string, object>(null, ""ERROR: Invalid workflow child mapping. Map does not match single implicit branch."");
                        }}
                        string nextFunction = childMap[0];
                        return new System.Tuple<string, object>(nextFunction, result);
                    }}",
            });
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
