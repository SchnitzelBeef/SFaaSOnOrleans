
namespace Infra.EcommerceFunctions
{
    public static class FunctionsHelper
    {
        public static string GetArgs(List<(Type, string)> args_out)
        {
            var code = $@"
                if (args.Length != {args_out.Count}) {{
                    var msg = ""Expected {args_out.Count} arguments, got "" + args.Length;
                    return new Tuple<int, object>(-1, msg);
                }}
             ";

            for (int i = 0; i < args_out.Count; i++)
            {
                string type = args_out[i].Item1.ToString();
                string name = args_out[i].Item2;
                code += $@"
                if (args[{i}] is not {type}) {{
                    var msg = ""Expected args[{i}] ({name}) to be type '{type}', got '"" + args[{i}]?.GetType().Name + ""'"";
                    return new Tuple<int, object>(-1, msg);
                }}
                var {name} = ({type})args[{i}];
                ";
            }
            return code;
        }
    }
}