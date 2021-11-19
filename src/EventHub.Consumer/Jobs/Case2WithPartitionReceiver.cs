using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Consumer.Jobs
{
    class Case2WithPartitionReceiver : IConsumer
    {
        private readonly EventHubConfiguration _configuration;

        public string Name => "Case 2: Read partition ('0','1','2') in bacth size using PartitionReceiver ...";

        public Case2WithPartitionReceiver(EventHubConfiguration configuration)
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
                partition > 3)
            {
                Console.WriteLine("Invalid partition ...");
                return;
            }

            var receiver = new PartitionReceiver(
                hub.ConsumerGroup,
                partition.ToString(),
                EventPosition.Earliest,
                _configuration.ConnectionString,
                hub.HubName);

            var cancellationSource = new CancellationTokenSource();

            int batchSize = 2500;

            Console.WriteLine($"Reading partition {partition} in bacth size of {batchSize} itens. Starting by earliest position. Press 'Escape' cancel ...");

            try
            {
                _ = Task.Factory.StartNew(() =>
                {
                    while (Console.ReadKey().Key != ConsoleKey.Escape)
                    {
                        continue;
                    }

                    Console.WriteLine("Cancelling ... ");

                    cancellationSource.Cancel();
                });

                while (!cancellationSource.IsCancellationRequested)
                {
                    var waitTime = TimeSpan.FromSeconds(5);

                    var eventBatch = await receiver.ReceiveBatchAsync(
                        batchSize,
                        waitTime,
                        cancellationSource.Token);

                    if (!eventBatch.Any())
                    {
                        Console.WriteLine("No messages found. Press 'Escape' cancel ...");
                        continue;
                    }

                    Application.Log(eventBatch, partition.ToString());
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                Console.WriteLine("Closing receiver ... ");

                await receiver.CloseAsync();
            }

            Console.WriteLine("Finished ... ");
        }
    }
}
