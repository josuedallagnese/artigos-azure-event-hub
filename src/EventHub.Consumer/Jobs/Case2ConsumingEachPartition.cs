using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Consumer.Jobs
{
    class Case2ConsumingEachPartition : IConsumer
    {
        private readonly EventHubConfiguration _configuration;

        public string Name => "Case 2: consuming each partition ('0','1','2') in sequence with EventHubConsumerClient";

        public Case2ConsumingEachPartition(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ... ");

            var hub = _configuration.GetHub("case2");

            var consumer = new EventHubConsumerClient(
                hub.ConsumerGroup,
                _configuration.ConnectionString,
                hub.HubName);

            try
            {
                var partitions = await consumer.GetPartitionIdsAsync();

                foreach (var partition in partitions)
                {
                    await ReadPartitionAsync(consumer, partition);
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                Console.WriteLine("Closing consumer... ");

                await consumer.CloseAsync();
            }

            Console.WriteLine("Finished ... ");
        }

        private static async Task ReadPartitionAsync(EventHubConsumerClient consumer, string partition)
        {
            var cancellationSource = new CancellationTokenSource();

            Console.WriteLine();
            Console.WriteLine($"Reading partition {partition}. Starting for earliest position...");

            var options = new ReadEventOptions()
            {
                MaximumWaitTime = TimeSpan.FromSeconds(1)
            };

            try
            {
                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsFromPartitionAsync(
                    partition,
                    EventPosition.Earliest,
                    options,
                    cancellationSource.Token))
                {
                    if (partitionEvent.Data == null)
                    {
                        Console.WriteLine("No messages found.");

                        cancellationSource.Cancel();

                        continue;
                    }

                    Application.Log(partitionEvent);
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }

            Console.WriteLine();
        }
    }
}
