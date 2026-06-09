using Confluent.Kafka;
using Infra.EventSchema;
using Infra.Service;
using Orleans.Concurrency;

namespace Infra.Kafka;

[Reentrant]
public class MediatorGrain : Grain, IMediatorGrain
{
    private long id;
    private string _topic;
    private IConsumer<string, Event> _consumer;

    private CancellationToken cancellationToken;

    private IProducer<string, Event> producer;
    private IProducer<long, Inventory> inventoryProducer;
    private IProducer<long, Checkout> checkoutProducer;
    private IProducer<long, Outcome> outcomeProducer;

    private List<IExecutorGrain> executors;
    private TaskScheduler scheduler;
    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        this.id = this.GetPrimaryKeyLong();
        // For some reason, using the provided cancellation token makes the consumer loop throw an error down the road
        // Creating a new one does not seem to cause errors
        // This is confusing
        this.cancellationToken = new CancellationToken(); // cancellationToken;
        this.scheduler = TaskScheduler.Current;
        return Task.CompletedTask;
    }

    public Task Init()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = Constants.KafkaService,
            GroupId = Constants.EventTopicGroup,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true,
        };

        this._consumer = new ConsumerBuilder<string, Event>(consumerConfig)
            .SetValueDeserializer(new EventSerializer<Event>())
            .Build();

        this._topic = Constants.EventTopic;
        this._consumer.Subscribe(this._topic);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = Constants.KafkaService,
            AllowAutoCreateTopics = true
        };

        this.producer = new ProducerBuilder<string, Event>(producerConfig)
            .SetValueSerializer(new EventSerializer<Event>())
            .Build();

        this.inventoryProducer = new ProducerBuilder<long, Inventory>(producerConfig)
            .SetValueSerializer(new EventSerializer<Inventory>())
            .Build();

        this.checkoutProducer = new ProducerBuilder<long, Checkout>(producerConfig)
            .SetValueSerializer(new EventSerializer<Checkout>())
            .Build();

        this.outcomeProducer = new ProducerBuilder<long, Outcome>(producerConfig)
            .SetValueSerializer(new EventSerializer<Outcome>())
            .Build();

        // HACK. See GetExecutor
        this.executors = new List<IExecutorGrain>();
        for (int i = 0; i < Constants.NumExecutorActors; i++)
        {
            this.executors.Add(GrainFactory.GetGrain<IExecutorGrain>(i));
        }

        Task.Run(() => StartConsuming(this.cancellationToken), this.cancellationToken);

        return Task.CompletedTask;
    }

    private IExecutorGrain GetExecutor()
    {
        var rng = new Random();

        // HACK. Since we're spawning a dotnet thread instead of using orleans, we cannot use GetGrain inside the thread.
        // Instead we must call GetGrain before and cache the results. This does mean if the handle becomes stale (idk if it can)
        // Then this won't work. 
        // A better solution would be to do like we did last assignment, using orleans timer and schedule.
        return executors[rng.Next(0, Constants.NumExecutorActors)];
        //return GrainFactory.GetGrain<IExecutorGrain>(rng.Next(0, Constants.NumExecutorActors));
    }


    public async Task<bool> StartWorkflow(string functionName, object[] parameters)
    {
        /*
        Console.WriteLine($"\nStarting workflow: {functionName}");
        foreach (var p in parameters)
            Console.WriteLine($"\tParameter type: {p?.GetType().FullName}, value: {p}");
        */

        var @event = new Event(functionName, parameters, true);
        return await ProduceNextWorkflow(@event);
    }

    private async Task<bool> ProduceNextWorkflow(Event @event)
    {
        // Assemble function name and parameters into an event object
        try
        {
            var res = await this.producer.ProduceAsync(this._topic, new Message<string, Event>
            {
                Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
                Key = @event.functionName,
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
        Console.WriteLine($"Started consuming from topic '{this._topic}'...");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = await Task.Run(() => this._consumer.Consume(cancellationToken));
                    await ProcessMessageAsync(consumeResult.Message.Key, consumeResult.Message.Value);
                    this._consumer.Commit(consumeResult);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
            Console.WriteLine("Cancelled was requested. Closing mediator.");
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
        /* TODO Handle the Kafka message.
         * In particular, (a) trigger an executor grain, (b) receive the function output, 
         * (c) assemble it into an event, and (d) publish it to the correct Kafka topic/partition. 
         * Make sure to acknowledge the processing of Kafka message correctly in order 
         * to ensure exactly-once processing.
         * The following link might be of interest:
         * https://learn.microsoft.com/en-us/dotnet/orleans/grains/external-tasks-and-grains#example-make-a-grain-call-from-code-running-on-a-thread-pool-thread
         */

        // ^^ I believe we are doing most of this in the code below, but we are not publishing to correct topic/partition

        if (@event == null || @event.functionName == null || @event.parameters == null)
        {
            Console.WriteLine($"Mediator got null event");
            return;
        }

        var functionName = @event.functionName;
        var parameters = @event.parameters;
        var isWorkflow = @event.isWorkflow;

        /*
        Console.WriteLine($"\nRecieved message: {functionName}. Is workflow: {isWorkflow}");
        foreach (var p in parameters)
        {
            Console.WriteLine($"\tParameter type: {p?.GetType().FullName}, value: {p}");
        }
        */

        // Trigger executor grain and await function output
        var result = await GetExecutor().Execute(functionName, parameters);

        if (isWorkflow)
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

            // Push next step in the workflow
            var workflowResult = (Tuple<string, object>)result;
            var nextfunction = workflowResult.Item1;

            // Ensure the output is an object[]
            object[] output;
            if (workflowResult.Item2 is object[])
                output = (object[])workflowResult.Item2;
            else
                output = new object[] { workflowResult.Item2 };

            if (nextfunction != null)
            {
                /*
                Console.WriteLine($"Producing next workflow: {functionName} -> {nextfunction}.");
                foreach (var p in output)
                    Console.WriteLine($"\tParameter type: {p?.GetType().FullName}, value: {p}");
                */

                var nextEvent = new Event(nextfunction, output, true);
                var _ = ProduceNextWorkflow(nextEvent);
            }

            // Forward events to kafka if applicable
            foreach (var obj in output)
            {
                if (obj is Inventory)
                {
                    var i = (Inventory)obj;
                    Console.WriteLine($"Got Inventory. customerId: {i.customerId}, price: {i.price}, quantity {i.quantity}");
                    await inventoryProducer.ProduceAsync(Constants.InventoryTopic, new Message<long, Inventory>
                    {
                        Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
                        Key = i.customerId,
                        Value = i
                    });
                }
                else if (obj is Checkout)
                {
                    var c = (Checkout)obj;
                    Console.WriteLine($"Got Checkout. productId: {c.productId}, price: {c.price}, quantity: {c.quantity}");
                    await checkoutProducer.ProduceAsync(Constants.CheckoutTopic, new Message<long, Checkout>
                    {
                        Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
                        Key = c.productId,
                        Value = c
                    });
                }
                else if (obj is Outcome)
                {
                    var o = (Outcome)obj;
                    Console.WriteLine($"Got Outcome. productId: {o.productId}, customerId: {o.customerId}, total: {o.total}, status: {o.status}");
                    await outcomeProducer.ProduceAsync(Constants.OutcomeTopic, new Message<long, Outcome>
                    {
                        Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
                        Key = o.customerId,
                        Value = o
                    });
                }
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
