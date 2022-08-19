namespace ProducerAPI
{
    using Confluent.Kafka;
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    public class ProducerWrapper
    {
        private string _topicName;
        private ProducerConfig _config;

        public ProducerWrapper(ProducerConfig config, string topicName)
        {
            this._topicName = topicName;
            this._config = config;
        }
        public async Task writeMessage(string message)
        {
            using (var p = new ProducerBuilder<string, string>(this._config).Build())
            {
                await p.ProduceAsync(this._topicName, new Message<string, string> { Key= DateTime.Now.ToString(), Value = message });

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
            return;
        }
    }
}
