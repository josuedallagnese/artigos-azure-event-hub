using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using EventHub.Core;
using EventHub.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventBus.Consumer
{
    public class Program
    {
        public static async Task Main()
        {
            var services = new ServiceCollection();

            var configuration = new ConfigurationBuilder()
                .AddJsonFile($"appsettings.Development.json")
                .Build();

            var eventHubConfiguration = new EventHubConfiguration(configuration);

            services.AddSingleton(eventHubConfiguration);

            Assembly.GetExecutingAssembly()
                .DefinedTypes
                .Where(x => x.GetInterfaces().Contains(typeof(IConsumer)))
                .ToList()
                .ForEach(type => services.Add(new ServiceDescriptor(typeof(IConsumer), type, ServiceLifetime.Transient)));

            using var scope = services.BuildServiceProvider().CreateScope();

            var jobs = scope.ServiceProvider.GetServices<IConsumer>().ToList();

            while (true)
            {
                Console.Clear();
                Console.WriteLine("What job do you want?");

                for (int i = 1; i <= jobs.Count; i++)
                {
                    Console.WriteLine($"[Option {i}] - {jobs[i - 1].Name}");
                }

                Console.WriteLine();
                Console.WriteLine("[0] - Exit");
                Console.WriteLine();

                _ = int.TryParse(Console.ReadLine().ToLower(), out var jobIndex);

                if (jobIndex == 0)
                    return;

                jobIndex--;

                if (jobIndex >= jobs.Count)
                {
                    Console.WriteLine("Invalid job!");
                    return;
                }

                var correlationId = Guid.NewGuid();

                await jobs[jobIndex].ExecuteAsync(correlationId);

                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }
    }
}
