using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

        public static async Task StoreMessagesAsync(IEnumerable<Case3Message> messages, EventHubConfiguration hub)
        {
            var ids = messages.Select(s => s.Id).ToArray();

            using var context = Create(hub);

            var exists = await context.Case3
                .Where(w => ids.Contains(w.Id))
                .Select(s => s.Id)
                .ToArrayAsync();

            if (exists.Length > 0)
                Console.ForegroundColor = ConsoleColor.Yellow;

            Console.WriteLine($"Exists: {exists.Length}");
            Console.ForegroundColor = ConsoleColor.White;

            var canBeInsert = messages
                .Where(w => !exists.Contains(w.Id))
                .ToArray();

            if (canBeInsert.Any())
            {
                context.Case3.AddRange(canBeInsert);

                await context.SaveChangesAsync();

                Console.WriteLine($"Inserted: {canBeInsert.Length}");
            }
        }

        public static async Task MigrateAsync(EventHubConfiguration hub)
        {
            using var context = Create(hub);

            Console.WriteLine("Apply migrations ...");

            await context.Database.MigrateAsync();

            Console.WriteLine("Database updated ...");
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
