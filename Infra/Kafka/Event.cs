namespace Infra.Kafka;

/**
 * This class must refer to the output of functions published and processed by Kafka.
 * TODO Implement this class
 */
public class Event
{
    public string functionName { get; set; }
    public object[] parameters { get; set; }
    public bool isWorkflow { get; set; }
    public Event(string functionName, object[] parameters, bool isWorkflow = false)
    {
        this.functionName = functionName;
        this.parameters = parameters;
        this.isWorkflow = isWorkflow;
    }
}
