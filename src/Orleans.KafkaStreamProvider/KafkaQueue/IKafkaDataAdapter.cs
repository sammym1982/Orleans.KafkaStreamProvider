using KafkaNet.Protocol;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public interface IKafkaDataAdapter
    {
        Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext);
        Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, T singleEvent, Dictionary<string, object> requestContext);
        KafkaBatchContainer FromKafkaMessage(Message message, long sequenceId);
    }

    public class KafkaDataAdapter : IKafkaDataAdapter, IOnDeserialized
    {
        private SerializationManager _serializationManager;

        public KafkaDataAdapter(SerializationManager serializationManager)
        {
            _serializationManager = serializationManager;
        }

        public Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            KafkaBatchContainer container = new KafkaBatchContainer(streamId, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = _serializationManager.SerializeToByteArray(container);
            Message message = new Message() { Value = rawBytes };

            return message;
        }

        public Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, T singleEvent, Dictionary<string, object> requestContext)
        {
            KafkaBatchContainer container = new KafkaBatchContainer(streamId, streamNamespace, singleEvent, requestContext);
            var rawBytes = _serializationManager.SerializeToByteArray(container);
            Message message = new Message() { Value = rawBytes };

            return message;
        }

        public KafkaBatchContainer FromKafkaMessage(Message message, long sequenceId)
        {
            var kafkaBatch = _serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(message.Value);
            kafkaBatch.SequenceToken = new EventSequenceToken(sequenceId);

            return kafkaBatch;
        }

        void IOnDeserialized.OnDeserialized(ISerializerContext context)
        {
            _serializationManager = context.GetSerializationManager();
        }
    }
}
