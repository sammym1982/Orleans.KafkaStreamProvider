using System;
using System.Collections.Generic;
using KafkaNet.Protocol;

namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public class KafkaBatchFactory : IKafkaBatchFactory
    {
        private IKafkaDataAdapter _dataAdapetr;

        public KafkaBatchFactory(IKafkaDataAdapter dataAdapter) => _dataAdapetr = dataAdapter;

        public Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            return _dataAdapetr.ToKafkaMessage(streamId, streamNamespace, events, requestContext);
        }

        public Orleans.Streams.IBatchContainer FromKafkaMessage(Message kafkaMessage, long sequenceId)
        {
            return _dataAdapetr.FromKafkaMessage(kafkaMessage, sequenceId);
        }

        public Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, T singleEvent, Dictionary<string,object> requestContext )
        {
            return _dataAdapetr.ToKafkaMessage(streamId, streamNamespace, singleEvent, requestContext);
        }
    }
}
