using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Consumer.Jobs
{
    class Case2ConsumingSinglePartition : IConsumer
    {
        private readonly EventHubConfiguration _configuration;

        public string Name => "Case 2: consuming a single partition EventHubConsumerClient";

        public Case2ConsumingSinglePartition(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ...");

            var hub = _configuration.GetHub("case2");

            Console.WriteLine("Which partition would you like?");

            if (!int.TryParse(Console.ReadLine(), out var partition) ||
                partition < 0 ||
                partition > 2)
            {
                Console.WriteLine("Invalid partition. Valid partitions: 0, 1 and 2 ...");
                return;
            }

            var consumer = new EventHubConsumerClient(
                hub.ConsumerGroup,
                _configuration.ConnectionString,
                hub.HubName);

            try
            {
                await ReadPartitionAsync(consumer, partition.ToString());
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                await consumer.CloseAsync();
            }

            Console.WriteLine("Finished ... ");
        }

        private static async Task ReadPartitionAsync(EventHubConsumerClient consumer, string partition)
        {
            var cancellationSource = new CancellationTokenSource();

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

                    LogEventData(partitionEvent);
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }

            Console.WriteLine();
        }

        private static void LogEventData(PartitionEvent @event)
        {
            var messageNumber = @event.Data.Properties["messageNumber"].ToString();
            var sequenceNumber = @event.Data.SequenceNumber;

            Console.WriteLine($"[MessageNumber: {messageNumber}] - [SequenceNumber: {sequenceNumber}] - [PartitionId: {@event.Partition.PartitionId}]");
        }
    }
}
