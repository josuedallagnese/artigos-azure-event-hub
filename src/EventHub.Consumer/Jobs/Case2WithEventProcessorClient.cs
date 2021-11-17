using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Consumer.Jobs
{
    class Case2WithEventProcessorClient : IConsumer
    {
        private readonly EventHubConfiguration _configuration;

        public string Name => "Case 2: all partitions with EventProcessorClient";

        public Case2WithEventProcessorClient(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ...");

            var hub = _configuration.GetHub("case2");

            var storageClient = new BlobContainerClient(
                hub.BlobStorageConnectionString,
                hub.BlobContainerName);

            var processor = new EventProcessorClient(
                storageClient,
                hub.ConsumerGroup,
                _configuration.ConnectionString,
                hub.HubName);

            Task InitializeEventHandler(PartitionInitializingEventArgs args)
            {
                try
                {
                    if (args.CancellationToken.IsCancellationRequested)
                        return Task.CompletedTask;

                    // If no checkpoint was found, start processing
                    // events enqueued now or in the future.

                    args.DefaultStartingPosition = EventPosition.Earliest;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

                return Task.CompletedTask;
            }

            Task CloseEventHandler(PartitionClosingEventArgs args)
            {
                try
                {
                    if (args.CancellationToken.IsCancellationRequested)
                        return Task.CompletedTask;

                    string description = args.Reason switch
                    {
                        ProcessingStoppedReason.OwnershipLost =>
                            "Another processor claimed ownership",

                        ProcessingStoppedReason.Shutdown =>
                            "The processor is shutting down",

                        _ => args.Reason.ToString()
                    };

                    Console.WriteLine($"Closing partition: {args.PartitionId}");
                    Console.WriteLine($"Reason: {description}");
                }
                catch
                {
                    // Take action to handle the exception.
                    // It is important that all exceptions are
                    // handled and none are permitted to bubble up.
                }

                return Task.CompletedTask;
            }

            async Task ProcessEventHandler(ProcessEventArgs args)
            {
                if (!args.HasEvent)
                    return;

                LogEventData(args);

                await args.UpdateCheckpointAsync();

                Console.WriteLine($"Update checkpoint - [PartitionId: {args.Partition.PartitionId}] - [SequenceNumber: {args.Data.SequenceNumber}]");
            }

            Task ProcessErrorHandler(ProcessErrorEventArgs args)
            {
                Console.WriteLine("Error in the EventProcessorClient");
                Console.WriteLine($"Operation: {args.Operation}");
                Console.WriteLine($"Exception: {args.Exception}");

                return Task.CompletedTask;
            }

            try
            {
                using var cancellationSource = new CancellationTokenSource();

                processor.PartitionInitializingAsync += InitializeEventHandler;
                processor.PartitionClosingAsync += CloseEventHandler;
                processor.ProcessEventAsync += ProcessEventHandler;
                processor.ProcessErrorAsync += ProcessErrorHandler;

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
                    Console.WriteLine(ex);
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

                processor.ProcessEventAsync -= ProcessEventHandler;
                processor.ProcessErrorAsync -= ProcessErrorHandler;
            }

            Console.WriteLine("Finished ... ");
        }

        private static void LogEventData(ProcessEventArgs args)
        {
            var id = args.Data.Properties["id"].ToString();
            var number = args.Data.Properties["number"].ToString();
            var sequenceNumber = args.Data.SequenceNumber;

            Console.WriteLine($"[id: {id}] - [Number: {number}] - [SequenceNumber: {sequenceNumber}] - [PartitionId: {args.Partition.PartitionId}]");
        }
    }
}
