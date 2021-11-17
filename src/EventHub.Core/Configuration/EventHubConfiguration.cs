using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace EventHub.Core.Configuration
{
    public class EventHubConfiguration
    {
        public string ConnectionString { get; set; }
        public string NpgsqlConnectionString { get; set; }
        public IEnumerable<HubConfiguration> Hubs { get; set; }

        public EventHubConfiguration(IConfiguration configuration)
        {
            ConnectionString = configuration.GetValue<string>("EventHub:ConnectionString");
            NpgsqlConnectionString = configuration.GetValue<string>("NpgsqlConnectionString");
            Hubs = configuration.GetSection("EventHub:Hubs").Get<IEnumerable<HubConfiguration>>();
        }

        public HubConfiguration GetHub(string hubName) => Hubs.Single(w => w.HubName == hubName);
    }

    public class HubConfiguration
    {
        public string HubName { get; set; }
        public string ConsumerGroup { get; set; }
        public int NumberOfMessages { get; set; }
        public int NumberOfMessagesByChunk { get; set; }
        public string BlobContainerName { get; set; }
        public string BlobStorageConnectionString { get; set; }
    }
}
