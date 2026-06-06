using Confluent.Kafka;
using Infra.Service;
using Orleans.Concurrency;
using Infra.Interfaces;
using Infra.Kafka;
using Microsoft.CodeAnalysis.CSharp.Syntax;
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
        this.cancellationToken = cancellationToken;
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
                    Console.WriteLine($"Consumed message. Key = {consumeResult.Message.Key}, Value = {consumeResult.Message.Value}");
                    await ProcessMessageAsync(consumeResult.Message.Key, consumeResult.Message.Value);
                    this._consumer.Commit();
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

    private async Task ProcessMessageAsync(string key, Event @event)
    {


        var functionName = @event.functionName;
        var parameters = @event.parameters;

        Console.WriteLine($"Received message: Key = {functionName}, Value = {parameters}");

        // Trigger executor grain and await function output
        var result = await executor.Execute(functionName, parameters);
        // var result = await Task.Factory.StartNew(
        //     () => executor.Execute(functionName, parameters),
        //     CancellationToken.None,
        //     TaskCreationOptions.None,
        //     scheduler 
        // ).Unwrap();

        // Assemble into event

        // Event @outputEvent = new Event(functionName, new object[] { result });
        
        var returnValue = await executor.Execute(functionName, parameters);
        if (returnValue is Event @newEvent)
        {
            Console.WriteLine($"Execution result: {newEvent.functionName}, {newEvent.parameters}");
            await this.StartEventWorkflow(@newEvent);
        }
        else
        {
            Console.WriteLine($"Terminal result: {returnValue}");
        }
        
        // Do stuff here    
    
        /* TODO Handle the Kafka message.
         * In particular, (a) trigger an executor grain, (b) receive the function output, 
         * (c) assemble it into an event, and (d) publish it to the correct Kafka topic/partition. 
         * Make sure to acknowledge the processing of Kafka message correctly in order 
         * to ensure exactly-once processing.
         * The following link might be of interest:
         * https://learn.microsoft.com/en-us/dotnet/orleans/grains/external-tasks-and-grains#example-make-a-grain-call-from-code-running-on-a-thread-pool-thread
         */
        return;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        this._consumer.Dispose();
        return Task.CompletedTask;
    }
}

