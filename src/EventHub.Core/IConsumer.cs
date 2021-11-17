using System;
using System.Threading.Tasks;

namespace EventHub.Core
{
    public interface IConsumer
    {
        string Name { get; }
        Task ExecuteAsync(Guid id);
    }
}
