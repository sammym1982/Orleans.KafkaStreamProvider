using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.KafkaStreamProvider.KafkaQueue;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;

namespace Orleans.KafkaStreamProvider.PersistentStreams
{
    public class KafkaStreamProvider : PersistentStreamProvider
    {
        public KafkaStreamProvider(
            string name,
            StreamPubSubOptions pubsubOptions,
            StreamLifecycleOptions lifeCycleOptions,
            IProviderRuntime runtime,
            SerializationManager serializationManager,
            ILogger<PersistentStreamProvider> logger) : base(name, pubsubOptions, lifeCycleOptions, runtime, serializationManager, logger)
        {
        }
    }
}
