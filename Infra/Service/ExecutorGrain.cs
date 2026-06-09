using Infra.EventSchema;
using Infra.Interfaces;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using System.Collections.Concurrent;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
namespace Infra.Service;

[Reentrant]
[StatelessWorker]
public class ExecutorGrain : Grain, IExecutorGrain
{
    public static readonly ConcurrentDictionary<string, (Assembly Assembly, Type Type, string Hash)> CompiledCache = new();

    private readonly IKeyValueStore kvs;

    // Use logger if necessary
    private readonly ILogger<ExecutorGrain> logger;

    public Task Init()
    {
        return Task.CompletedTask;
    }

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        return;
    }

    public ExecutorGrain(IKeyValueStore kvs, ILogger<ExecutorGrain> logger)
    {
        this.kvs = kvs;
        this.logger = logger;
    }

    public Task<object> Execute(string functionName, object[] parameters)
    {
        var code = this.kvs.GetString(functionName) ?? throw new Exception($"Function '{functionName}' not found.");
        return Task.FromResult(DoExecute(functionName, code, this.kvs, parameters));
    }

    // Dynamically compile and execute the function with registry context
    private static object DoExecute(string functionName, string code, IKeyValueStore kvs, object[] parameters)
    {
        string newHash = GetCodeHash(code);
        string oldHash = null;
        if (CompiledCache.TryGetValue(functionName, out var oldCached))
        {
            oldHash = oldCached.Hash;
        }

        if (oldHash == null || newHash != oldHash)
        {
            Console.WriteLine($"Compiling function '{functionName}'");
            // Wrap user code in a class and method
            string wrappedCode = $@"
            using System;
            using System.Reflection;
            using System.Linq;
            using System.Collections.Generic;
            using Microsoft.CodeAnalysis;
            using Microsoft.CodeAnalysis.CSharp;
            using Infra.Interfaces;
            using Infra.EventSchema;
            using Infra.EcommerceStates;
    
            public class DynamicClass
            {{
                private readonly IKeyValueStore kvs;
                
                public DynamicClass(IKeyValueStore kvs){{
                    this.kvs = kvs;
                }}
                
                public object Execute(params object[] args)
                {{
                    {code}
                    return null; // Fallback for functions not returning result
                }}
            }}";
            // Compile the assembly
            var assembly = CompileAssembly(functionName, wrappedCode);
            var type = assembly.GetType("DynamicClass");

            // Cache the compiled assembly and type
            CompiledCache[functionName] = (assembly, type, newHash);
        }

        // Use the cached assembly to execute the function
        Console.WriteLine($"\nExecuting function '{functionName}'");
        var cached = CompiledCache[functionName];
        var instance = Activator.CreateInstance(cached.Type, kvs);
        var method = cached.Type.GetMethod("Execute");
        var result = method.Invoke(instance, new object[] { parameters });
        Console.WriteLine($"Execution result: {result}");
        return result;
    }

    public static string GetCodeHash(string code)
    {
        return Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(code))
        );
    }

    public static Assembly CompileAssembly(string functionName, string code)
    {
        // Compile the wrapped code
        var syntaxTree = CSharpSyntaxTree.ParseText(code);
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .ToList();

        references.Add(MetadataReference.CreateFromFile(typeof(Checkout).Assembly.Location));

        var compilation = CSharpCompilation.Create(
            $"{functionName}_Assembly",
            new[] { syntaxTree },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        using var ms = new MemoryStream();
        var result = compilation.Emit(ms);

        if (!result.Success)
        {
            var errors = result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error);
            throw new InvalidOperationException(string.Join("\n", errors.Select(e => e.GetMessage())));
        }

        ms.Seek(0, SeekOrigin.Begin);
        return Assembly.Load(ms.ToArray());
    }
}
