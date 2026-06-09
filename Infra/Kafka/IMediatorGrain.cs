namespace Infra.Kafka;

public interface IMediatorGrain : IGrainWithIntegerKey
{
    Task Init();
    Task<bool> StartWorkflow(string functionName, object[] parameters);
}
