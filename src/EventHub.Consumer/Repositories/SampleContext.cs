using EventHub.Core;
using EventHub.Core.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;

namespace EventHub.Consumer.Repositories
{
    public class SampleContext : DbContext
    {
        public string ConnectionString { get; }

        public DbSet<Case3Message> Case3 { get; set; }

        public SampleContext(DbContextOptions options) :
            base(options)
        {
        }

        public static SampleContext Create(EventHubConfiguration hub)
        {
            var connectionString = hub.NpgsqlConnectionString;

            var dbContextOptionsBuilder = new DbContextOptionsBuilder()
                .UseNpgsql(connectionString);

            return new SampleContext(dbContextOptionsBuilder.Options);
        }
    }

    public class DesignTimeSampleContextFactory : IDesignTimeDbContextFactory<SampleContext>
    {
        public SampleContext CreateDbContext(string[] args)
        {
            var configuration = new ConfigurationBuilder()
               .AddJsonFile($"appsettings.Development.json")
               .Build();

            var eventHubConfiguration = new EventHubConfiguration(configuration);

            return SampleContext.Create(eventHubConfiguration);
        }
    }
}
