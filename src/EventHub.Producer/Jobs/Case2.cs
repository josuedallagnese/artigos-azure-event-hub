using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using EventHub.Core;
using EventHub.Core.Configuration;

namespace EventHub.Producer.Jobs
{
    class Case2 : IProducer
    {
        public string Name => "Case 2: producing messages to hub with three partitions";

        private readonly EventHubConfiguration _configuration;

        public Case2(EventHubConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task ExecuteAsync(Guid id)
        {
            Console.WriteLine("Starting ... ");

            var hub = _configuration.GetHub("case2");

            var producer = new EventHubProducerClient(_configuration.ConnectionString, hub.HubName);

            using var eventBatch = await producer.CreateBatchAsync();

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

            foreach (var message in messages)
            {
                var eventBody = new BinaryData(message);
                var eventData = new EventData(eventBody);

                eventData.Properties.Add("producerId", id);
                eventData.Properties.Add("messageNumber", message.Id);

                if (!eventBatch.TryAdd(eventData))
                {
                    throw new Exception($"The event could not be added. See MaximumSizeInBytes in CreateBatchOptions.");
                }
            }

            Console.WriteLine($"Sending {eventBatch.Count} messages ... ");

            await producer.SendAsync(eventBatch);

            Console.WriteLine("Closing producer ... ");

            await producer.CloseAsync();

            Console.WriteLine("Finished ... ");
        }
    }
}
