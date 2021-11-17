using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using EventHub.Consumer.Repositories;
using EventHub.Core;
using EventHub.Core.Configuration;
using Microsoft.EntityFrameworkCore;

namespace EventHub.Consumer.Jobs
{
    class Case3 : IConsumer
    {
        private readonly EventHubConfiguration _configuration;

        public string Name => "Case 3: Read for partition in bacth size using PartitionReceiver and save in local database...";

        public Case3(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ...");

            using (var context = SampleContext.Create(_configuration))
            {
                Console.WriteLine("Apply migrations ...");

                await context.Database.MigrateAsync();

                Console.WriteLine("Database updated ...");
            }

            var hub = _configuration.GetHub("case3");

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

            try
            {
                await ReadPartitionAsync(_configuration, receiver, partition);
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

        private static async Task ReadPartitionAsync(EventHubConfiguration configuration, PartitionReceiver receiver, int partition)
        {
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

                    var modulos = eventBatch
                        .Select(s => new { s.SequenceNumber, Message = s.EventBody.ToObjectFromJson<Case3Message>() });

                    var modulosIds = modulos.Select(s => s.Message.Id).ToArray();

                    using (var context = SampleContext.Create(configuration))
                    {

                        var exists = await context.Case3
                            .Where(w => modulosIds.Contains(w.Id))
                            .Select(s => s.Id)
                            .ToArrayAsync();

                        if (exists.Length > 0)
                            Console.ForegroundColor = ConsoleColor.Yellow;

                        Console.WriteLine($"Already exists: {exists.Length}");
                        Console.ForegroundColor = ConsoleColor.White;

                        var canBeInsert = modulos
                            .Where(w => !exists.Contains(w.Message.Id))
                            .ToArray();

                        if (canBeInsert.Any())
                        {
                            context.Case3.AddRange(canBeInsert.Select(s => s.Message).ToArray());

                            await context.SaveChangesAsync();

                            Console.WriteLine($"Inserted: {canBeInsert.Length}");
                            Console.WriteLine($"Min SequenceNumber: {canBeInsert.Min(m => m.SequenceNumber)}");
                            Console.WriteLine($"Max SequenceNumber: {canBeInsert.Max(m => m.SequenceNumber)}");
                        }
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
        }
    }
}
