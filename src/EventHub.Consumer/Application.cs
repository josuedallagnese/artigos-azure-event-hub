using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace EventHub.Consumer
{
    public static class Application
    {
        public static void Log(EventData data, string partitionId)
        {
            var producerId = data.Properties["producerId"].ToString();
            var messageNumber = data.Properties["messageNumber"].ToString();
            var sequenceNumber = data.SequenceNumber;

            Console.WriteLine($"[ProducerId: {producerId}] - [MessageNumber: {messageNumber}] - [SequenceNumber: {sequenceNumber}] - [PartitionId: {partitionId}]");
        }

        public static void Log(IEnumerable<EventData> events, string partitionId)
        {
            foreach (var @event in events)
                Log(@event, partitionId);
        }

        public static void Log(PartitionEvent partitionEvent) => Log(partitionEvent.Data, partitionEvent.Partition.PartitionId);

        public static void Log(ProcessEventArgs args) => Log(args.Data, args.Partition.PartitionId);

        public static void Heartbeat(ProcessEventArgs _) => Console.WriteLine("Heartbeat");

        public static void HandleException(Exception exception)
        {
            Console.WriteLine(exception.Message);
        }

        public static Task EventProcessorClient_CloseEventHandler(PartitionClosingEventArgs args)
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

        public static Task EventProcessorClient_InitializeEventHandler(PartitionInitializingEventArgs args)
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
                HandleException(ex);
            }

            return Task.CompletedTask;
        }

        public static Task EventProcessorClient_ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Error in the EventProcessorClient");
            Console.WriteLine($"Operation: {args.Operation}");
            Console.WriteLine($"Exception: {args.Exception}");
            Console.ForegroundColor = ConsoleColor.White;

            return Task.CompletedTask;
        }
    }
}
