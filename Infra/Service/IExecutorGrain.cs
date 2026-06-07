namespace Infra.Service;

public interface IExecutorGrain : IGrainWithIntegerKey
{
    Task<object> Execute(string functionName, object[] parameters);

    // not supposed to be acessed by other actors, it is an API for clients
    Task Init();
}
