using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using EventHub.Consumer.Repositories;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Consumer.Jobs
{
    class Case3 : IConsumer
    {
        private readonly EventHubConfiguration _configuration;
        private readonly ConcurrentDictionary<string, List<EventData>> _batches;
        private bool _checkpointNeeded = false;
        private int _checkpointAt = 0;

        public string Name => "Case 3: Read all partition with Event Processor Client and save in local database...";

        public Case3(EventHubConfiguration configuration)
        {
            _configuration = configuration;

            _batches = new ConcurrentDictionary<string, List<EventData>>();
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ...");

            await SampleContext.MigrateAsync(_configuration);

            var hub = _configuration.GetHub("case3");

            var storageClient = new BlobContainerClient(
                hub.BlobStorageConnectionString,
                hub.BlobContainerName);

            var processor = new EventProcessorClient(
                storageClient,
                hub.ConsumerGroup,
                _configuration.ConnectionString,
                hub.HubName);

            _checkpointAt = hub.CheckpointAt;

            Console.WriteLine($"The Checkpoint process will be done At {_checkpointAt} messages. You can put another value or press any key to continue.");

            if (int.TryParse(Console.ReadLine(), out int anotherValue))
            {
                _checkpointAt = anotherValue;

                Console.WriteLine($"Checkpoint changed to {_checkpointAt} messages.");
            }

            try
            {
                using var cancellationSource = new CancellationTokenSource();

                processor.PartitionInitializingAsync += Application.EventProcessorClient_InitializeEventHandler;
                processor.PartitionClosingAsync += Application.EventProcessorClient_CloseEventHandler;
                processor.ProcessEventAsync += ProcessEventAsync;
                processor.ProcessErrorAsync += Application.EventProcessorClient_ProcessErrorHandler;

                try
                {
                    await processor.StartProcessingAsync(cancellationSource.Token);

                    Console.WriteLine("Reading hub messages for earliest position ... Press any key to cancel");
                    Console.ReadKey();

                    Console.WriteLine("Cancelling ... ");

                    cancellationSource.Cancel();
                }
                catch (TaskCanceledException)
                {
                    // This is expected if the cancellation token is
                    // signaled.
                }
                catch (Exception ex)
                {
                    Application.HandleException(ex);
                }
                finally
                {
                    // This may take up to the length of time defined
                    // as part of the configured TryTimeout of the processor;
                    // by default, this is 60 seconds.

                    Console.WriteLine("Stopping ... ");

                    await processor.StopProcessingAsync();
                }
            }
            catch
            {
                // The processor will automatically attempt to recover from any
                // failures, either transient or fatal, and continue processing.
                // Errors in the processor's operation will be surfaced through
                // its error handler.
                //
                // If this block is invoked, then something external to the
                // processor was the source of the exception.
            }
            finally
            {
                // It is encouraged that you unregister your handlers when you have
                // finished using the Event Processor to ensure proper cleanup.  This
                // is especially important when using lambda expressions or handlers
                // in any form that may contain closure scopes or hold other references.

                processor.ProcessEventAsync -= ProcessEventAsync;
                processor.ProcessErrorAsync -= Application.EventProcessorClient_ProcessErrorHandler;
            }

            Console.WriteLine("Finished ... ");
        }

        private async Task ProcessEventAsync(ProcessEventArgs args)
        {
            Application.Log(args);

            if (!args.HasEvent)
            {
                Application.Heartbeat(args);

                return;
            }

            var partition = args.Partition.PartitionId;

            var partitionBatch = _batches.GetOrAdd(
                partition,
                new List<EventData>());

            partitionBatch.Add(args.Data);

            if (partitionBatch.Count >= _checkpointAt)
            {
                var messages = partitionBatch.Select(s => s.EventBody.ToObjectFromJson<Case3Message>());

                await SampleContext.StoreMessagesAsync(messages, _configuration);

                _checkpointNeeded = true;

                partitionBatch.Clear();
            }

            if (_checkpointNeeded)
            {
                await args.UpdateCheckpointAsync();

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine();
                Console.WriteLine("Checkpoint done!");
                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.White;

                _checkpointNeeded = false;
            }
        }
    }
}
