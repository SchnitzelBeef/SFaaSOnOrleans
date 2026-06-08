using Confluent.Kafka;
using Infra.EventSchema;
using Infra.Service;
using Orleans.Concurrency;

namespace Infra.Kafka;

[Reentrant]
public class MediatorGrain : Grain, IMediatorGrain
{
    private IConsumer<string, Event> _consumer;

    private string _topic;
    private int _partition;
    private Offset _offset;

    private CancellationToken cancellationToken;

    private IProducer<Null, Event> producer;
    private TopicPartition topicPartition;

    private EventSerializer serializer = new EventSerializer();

    private TaskScheduler scheduler;
    private IExecutorGrain executor;
    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        // For some reason, using the provided cancellation token makes the consumer loop throw an error down the road
        // Creating a new one does not seem to cause errors
        // This is confusing
        this.cancellationToken = new CancellationToken(); // cancellationToken;
        this.scheduler = TaskScheduler.Current;
        return Task.CompletedTask;
    }

    public Task Init(string Topic, string GroupId, int Partition, int offset)
    {
        this._topic = Topic;
        this._partition = Partition;

        if (offset >= 0)
            this._offset = new Offset(offset);
        else
            this._offset = Offset.End;

        var consumerConfig = new ConsumerConfig
        {
            GroupId = GroupId,
            BootstrapServers = Constants.KafkaService,
            EnableAutoCommit = false
        };

        this._consumer = new ConsumerBuilder<string, Event>(consumerConfig)
            .SetValueDeserializer(new EventSerializer())
            .Build();

        var topicPartitionOffset =
                  new TopicPartitionOffset(this._topic, new Partition(this._partition), this._offset);

        this._consumer.Assign(topicPartitionOffset);

        this.executor = GrainFactory.GetGrain<IExecutorGrain>(0);

        Task.Run(() => StartConsuming(this.cancellationToken), this.cancellationToken);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = Constants.KafkaService,
            AllowAutoCreateTopics = true
        };

        this.producer = new ProducerBuilder<Null, Event>(producerConfig)
            .SetValueSerializer(new EventSerializer())
            .Build();

        // Define topic partition, should be better partitioned
        // currently we just use one partition for simplicity
        this.topicPartition = new TopicPartition(this._topic, new Partition(this._partition));

        return Task.CompletedTask;
    }


    public async Task<bool> StartWorkflow(string functionName, object[] parameters)
    {
        var @event = new Event(functionName, parameters, true);
        return await ProduceNextWorkflow(@event);
    }

    private async Task<bool> ProduceNextWorkflow(Event @event)
    {
        // Assemble function name and parameters into an event object
        try
        {
            Console.WriteLine($"EVENT: {@event.functionName} : {@event.parameters}");
            foreach (var p in @event.parameters)
                Console.WriteLine($"Parameter type: {p?.GetType().FullName}, value: {p}");

            var res = await this.producer.ProduceAsync(this.topicPartition, new Message<Null, Event>
            {
                Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
                Value = @event
            });
            return res.Status == PersistenceStatus.Persisted;
        }
        catch (ProduceException<Null, Event> ex)
        {
            Console.WriteLine($"Produce failed: {ex.Error.Reason}");
            Console.WriteLine($"Inner exception: {ex.InnerException}");
            return false; // or throw a plain Exception that Orleans can serialize
        }
    }

    private async Task StartConsuming(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Started consuming from topic '{this._topic}', partition {this._partition}, starting at offset {this._offset.Value}...");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = await Task.Run(() => this._consumer.Consume(cancellationToken));
                    // Console.WriteLine($"Consumed message. Value = {consumeResult.Message.Value}");
                    await ProcessMessageAsync(consumeResult.Message.Key, consumeResult.Message.Value);
                    // this._consumer.Commit();
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consumer loop error: {ex}");
        }
        finally
        {
            this._consumer.Close();
        }
    }

    private static object[] NormalizeParameters(object[] parameters)
    {
        return parameters.Select(p =>
        {
            if (p is Newtonsoft.Json.Linq.JArray jArray)
                return jArray.ToObject<object[]>();
            if (p is Newtonsoft.Json.Linq.JObject jObj)
                return jObj;
            return p;
        }).ToArray();
    }

    private async Task ProcessMessageAsync(string key, Event @event)
    {
        /* TODO Handle the Kafka message.
         * In particular, (a) trigger an executor grain, (b) receive the function output, 
         * (c) assemble it into an event, and (d) publish it to the correct Kafka topic/partition. 
         * Make sure to acknowledge the processing of Kafka message correctly in order 
         * to ensure exactly-once processing.
         * The following link might be of interest:
         * https://learn.microsoft.com/en-us/dotnet/orleans/grains/external-tasks-and-grains#example-make-a-grain-call-from-code-running-on-a-thread-pool-thread
         */

        // ^^ I believe we are doing most of this in the code below, but we are not publishing to correct topic/partition


        var functionName = @event.functionName;
        var parameters = @event.parameters;

        Console.WriteLine($"Received message: Key = {functionName}, Value = {parameters}, Is Workflow = {@event.isWorkflow}");
            
        // Trigger executor grain and await function output
        var result = await executor.Execute(functionName, parameters);
        Console.WriteLine($"Terminal result: {result}");

        if (@event.isWorkflow)
        {
            if (result is null)
            {
                Console.WriteLine($"Workflow result is null for function {functionName}");
                return;
            }
            if (result is not Tuple<string, object>)
            {
                Console.WriteLine($"Workflow result is not valid for function {functionName}");
                return;
            }

            var workflowResult = (Tuple<string, object>)result;
            var nextfunction = workflowResult.Item1;
            var output = workflowResult.Item2;
            if (nextfunction != null)
            {
                var nextEvent = new Event(nextfunction, new object[] { output }, true);
                var _ = ProduceNextWorkflow(nextEvent);
            }

            // TODO, handle Kafka here
            if (output is Inventory)
            {
                var i = (Inventory)output;
                Console.WriteLine($"Got Inventory request. customerId: {i.customerId}, price: {i.price}, quantity {i.quantity}");
            }
            else if (output is Checkout)
            {
                var c = (Checkout)output;
                Console.WriteLine($"Got Checkout request. productId: {c.productId}, price: {c.price}, quantity: {c.quantity}");
            }
            else if (output is Outcome)
            {
                var o = (Outcome)output;
                Console.WriteLine($"Got Outcome request. productId: {o.productId}, customerId: {o.customerId}, total: {o.total}, status: {o.status}");
            }
        }

        return;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        this._consumer.Dispose();
        return Task.CompletedTask;
    }
}
