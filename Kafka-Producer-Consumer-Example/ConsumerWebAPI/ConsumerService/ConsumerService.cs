using Confluent.Kafka;
using ConsumerWebAPI.Model;
using Newtonsoft.Json;

namespace ConsumerWebAPI.ConsumerService
{
    public class ConsumerService : BackgroundService
    {
        private readonly string topic;
        private readonly IConsumer<string, string> kafkaConsumer;
        public ConsumerService(IConfiguration config)
        {
            var consumerConfig = new ConsumerConfig();
            //config.GetSection("Kafka:ConsumerSettings").Bind(consumerConfig);
            consumerConfig.BootstrapServers = "localhost:9092";
            consumerConfig.GroupId = "welcometopicConsumer";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            this.topic = "welcometopic5";
            this.kafkaConsumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            //return Task.Delay(1);
        }

        private void StartConsumerLoop(CancellationToken cancellationToken)
        {
            kafkaConsumer.Subscribe(this.topic);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = this.kafkaConsumer.Consume(cancellationToken);
                   OrderRequest? order = JsonConvert.DeserializeObject<OrderRequest>(cr.Message.Value);
                    // Handle message...
                   Console.WriteLine($"{cr.Message.Key}: {cr.Message.Value}");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Unexpected error: {e}");
                    break;
                }
            }
        }

        public override void Dispose()
        {
            this.kafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
            this.kafkaConsumer.Dispose();

            base.Dispose();
        }

    }
}
