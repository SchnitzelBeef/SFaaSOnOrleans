namespace Infra.Kafka;

public interface IMediatorGrain : IGrainWithStringKey
{
    Task Init(string Topic, string GroupId, int Partition, int Offset);
    Task<bool> StartWorkflow(string functionName, object[] parameters);
}

