using System;
using System.Threading.Tasks;

namespace EventHub.Core
{
    public interface IProducer
    {
        string Name { get; }
        Task ExecuteAsync(Guid id);
    }
}
