using Confluent.Kafka;
using Infra.Service;
using Orleans.Concurrency;
using Infra.Interfaces;
using Infra.Kafka;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Infra.EventSchema;

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

        var producerConfig = new ProducerConfig {
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

    private async Task<bool> StartEventWorkflow(Event @event)
	{
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


    // Turned StartWorkflow into public wrapper method 
    public async Task<bool> StartWorkflow(string functionName, object[] parameters)
	{
        // Assemble function name and parameters into an event object
        var @event = new Event(functionName, parameters);
        return await StartEventWorkflow(@event);
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
                return (object)jArray.ToObject<object[]>();
            if (p is Newtonsoft.Json.Linq.JObject jObj)
                return (object)jObj;
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

        // Console.WriteLine($"Received message: Key = {functionName}, Value = {parameters}");

        // Trigger executor grain and await function output
        var result = await executor.Execute(functionName, parameters);

        // Check if result is part of a workflow, in which case an Event type is returned
        if (result is Event @newEvent)
        {
            if (newEvent.functionName == "ProcessInventoryRequest")
            {

                // Super not cool way to do this
                // This is because I cannot figure out how to return "Inventory" directly
                var p = (object[])NormalizeParameters(newEvent.parameters)[0];
                var productId = p[0]; // Convert.ToInt64(newEvent.parameters[0]);
                var customerId = (long)p[1];
                var price = (double)p[2]; // Convert.ToDouble(newEvent.parameters[1]);
                var quantity = Convert.ToInt32(p[3]); // Convert.ToInt32(newEvent.parameters[2]);
                newEvent.parameters = new object[] { productId, new Inventory(customerId, price, quantity) };
            }
            Console.WriteLine($"Intermediate execution result: {@newEvent.functionName}, {@newEvent.parameters}");
            await this.StartEventWorkflow(@newEvent);
        }
        else
        {
            Console.WriteLine($"Terminal result: {result}");
        }
    
        return;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        this._consumer.Dispose();
        return Task.CompletedTask;
    }
}

