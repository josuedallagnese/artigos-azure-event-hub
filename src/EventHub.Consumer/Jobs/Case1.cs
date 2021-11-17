using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Consumer.Jobs
{
    class Case1 : IConsumer
    {
        private readonly EventHubConfiguration _configuration;

        public string Name => "Case 1: consuming one partition ('0') with PartitionReceiver in batch size of maximum 10 itens";

        public Case1(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ... ");

            var hub = _configuration.GetHub("case1");

            using CancellationTokenSource cancellationSource = new CancellationTokenSource();

            var receiver = new PartitionReceiver(
                hub.ConsumerGroup,
                "0",
                EventPosition.Earliest,
                _configuration.ConnectionString,
                hub.HubName);

            try
            {
                while (true)
                {
                    int batchSize = 10;
                    TimeSpan waitTime = TimeSpan.FromSeconds(1);

                    IEnumerable<EventData> eventBatch = await receiver.ReceiveBatchAsync(
                        batchSize,
                        waitTime,
                        cancellationSource.Token);

                    if (eventBatch.Any())
                    {
                        Console.WriteLine();
                        Console.WriteLine("Chunk begin");

                        foreach (EventData eventData in eventBatch)
                        {
                            LogEventData(eventData, "0");
                        }

                        Console.WriteLine("Chunk end");
                        Console.WriteLine();
                    }
                    else
                    {
                        Console.WriteLine("No messages found. Press ESC to cancel!");

                        if (Console.ReadKey().Key == ConsoleKey.Escape)
                        {
                            cancellationSource.Cancel();
                            Console.WriteLine();
                        }
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                Console.WriteLine("Closing receiver... ");

                await receiver.CloseAsync();
            }

            Console.WriteLine("Finished ... ");
        }

        private static void LogEventData(EventData data, string partitionId)
        {
            var messageNumber = data.Properties["messageNumber"].ToString();
            var sequenceNumber = data.SequenceNumber;

            Console.WriteLine($"[MessageNumber: {messageNumber}] - [SequenceNumber: {sequenceNumber}] - [PartitionId: {partitionId}]");
        }
    }
}
