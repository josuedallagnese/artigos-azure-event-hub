using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Producer.Jobs
{
    public class Case3 : IProducer
    {
        public string Name => "Case 3: producing messages to hub with four partitions";

        private readonly EventHubConfiguration _configuration;

        public Case3(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ...");

            var hub = _configuration.GetHub("case3");

            Console.WriteLine($"Creating {hub.NumberOfMessages} random messages ...");

            var now = DateTimeOffset.Now;

            var messages = Enumerable.Range(1, hub.NumberOfMessages)
                .Select(i => new Case3Message
                {
                    Id = i,
                    Code = i.ToString().PadRight(10, '0'),
                    Employee = i,
                    Date = now.AddMilliseconds(-i),
                    Ticket = i,
                    Tax = i
                }).ToArray();

            var producer = new EventHubProducerClient(_configuration.ConnectionString, hub.HubName);

            var messageNumber = 1;
            var pageIndex = 1;
            var pages = messages.Batch(hub.Chunks);
            var totalPages = pages.Count() / hub.Chunks;

            Console.WriteLine($"Starting pages with size of {hub.Chunks}.");

            foreach (var page in pages)
            {
                var eventBatch = await producer.CreateBatchAsync();

                foreach (var message in page)
                {
                    var eventBody = new BinaryData(message);
                    var eventData = new EventData(eventBody);

                    eventData.Properties.Add("producerId", id);
                    eventData.Properties.Add("messageNumber", message.Id);

                    if (!eventBatch.TryAdd(eventData))
                    {
                        throw new Exception($"The event could not be added. See MaximumSizeInBytes in CreateBatchOptions.");
                    }

                    messageNumber++;
                }

                Console.WriteLine($"Sending {pageIndex} of {totalPages}");

                await producer.SendAsync(eventBatch);

                eventBatch.Dispose();

                pageIndex++;
            }

            Console.WriteLine("Closing producer ... ");

            await producer.CloseAsync();

            Console.WriteLine("Finished ... ");
        }
    }
}
