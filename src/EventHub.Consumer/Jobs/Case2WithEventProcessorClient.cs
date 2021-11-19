using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
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

            static Task ProcessEventAsync(ProcessEventArgs args)
            {
                if (!args.HasEvent)
                {
                    Application.Heartbeat(args);

                    return Task.CompletedTask;
                }

                Application.Log(args);

                return Task.CompletedTask;
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
    }
}
