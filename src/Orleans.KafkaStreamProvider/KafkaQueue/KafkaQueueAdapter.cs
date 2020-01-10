﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Metrics;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public class KafkaQueueAdapter : IQueueAdapter
    {
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly ProtocolGateway _gateway;
        private readonly IKafkaBatchFactory _batchFactory;
        private readonly ILogger _logger;
        private readonly KafkaStreamProviderOptions _options;
        private readonly Producer _producer;

        // Metrics
        private static readonly Meter MeterProducedMessagesPerSecond = Metrics.Metric.Context("KafkaStreamProvider").Meter("Produced Messages Per Second", Unit.Events);
        private static readonly Histogram HistogramProducedMessageBatchSize = Metrics.Metric.Context("KafkaStreamProvider").Histogram("Produced Message Batch Size", Unit.Events);
        private static readonly Timer TimerTimeToProduceMessage = Metrics.Metric.Context("KafkaStreamProvider").Timer("Time To Produce Message", Unit.Custom("Produces"));
    
        public bool IsRewindable => true;

        public string Name { get; }

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public KafkaQueueAdapter(HashRingBasedStreamQueueMapper queueMapper, KafkaStreamProviderOptions options,
            string providerName, IKafkaBatchFactory batchFactory, ILogger logger)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (batchFactory == null) throw new ArgumentNullException(nameof(batchFactory));
            if (queueMapper == null) throw new ArgumentNullException(nameof(queueMapper));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            if (string.IsNullOrEmpty(providerName)) throw new ArgumentNullException(nameof(providerName));

            _options = options;
            _streamQueueMapper = queueMapper;
            Name = providerName;
            _batchFactory = batchFactory;
            _logger = logger;

            // Creating a producer
            KafkaOptions kafkaOptions = new KafkaOptions(_options.ConnectionStrings.ToArray())
            {
                Log = new KafkaLogBridge(logger)
            };
            var broker = new BrokerRouter(kafkaOptions);
            _producer = new Producer(broker)
            {
                BatchDelayTime = TimeSpan.FromMilliseconds(_options.TimeToWaitForBatchInMs),
                BatchSize = _options.ProduceBatchSize
            };
            _gateway = new ProtocolGateway(kafkaOptions);

            _logger.Info("KafkaQueueAdapter - Created a KafkaQueueAdapter");
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            var partitionId = (int)queueId.GetNumericId();
            var clientName = Environment.MachineName + AppDomain.CurrentDomain.FriendlyName;

            // Creating a receiver
            var manualConsumer = new ManualConsumer(partitionId, _options.TopicName, _gateway, clientName, _options.MaxBytesInMessageSet);
            var receiverToReturn = new KafkaQueueAdapterReceiver(queueId, manualConsumer, _options, _batchFactory, _logger);

            return receiverToReturn;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            
            var partitionId = (int)queueId.GetNumericId();

            _logger.Info("KafkaQueueAdapter - For StreamId: {0}, StreamNamespace:{1} using partition {2}", streamGuid, streamNamespace, partitionId);

            var enumeratedEvents = events.ToList();
            var payload = _batchFactory.ToKafkaMessage(streamGuid, streamNamespace, enumeratedEvents.AsEnumerable(), requestContext);            
            if (payload == null)
            {
                _logger.Info("The batch factory returned a faulty message, the message was not sent");
                return;
            }

            var messageToSend = new List<Message> {payload};

            IEnumerable<ProduceResponse> response;

            using (TimerTimeToProduceMessage.NewContext())
            {
                response = await _producer.SendMessageAsync(_options.TopicName, messageToSend, (short) _options.AckLevel, partition: partitionId);
            }

            // This is ackLevel != 0 check
            if (response != null && !response.Contains(null))
            {
                var responsesWithError =
                    response.Where(messageResponse => messageResponse.Error != (int) ErrorResponseCode.NoError).ToList();
               
                if (responsesWithError.Any())
                {
                    List<Exception> allResponsesExceptions = new List<Exception>();

                    // Checking all the responses
                    foreach (var messageResponse in responsesWithError)
                    {
                        _logger.Warn((int)KafkaErrorCodes.KafkaStreamProviderBase,
                            "KafkaQueueAdapter - Error sending message through kafka client, the error code is {0}, message offset is {1}",
                            messageResponse.Error, messageResponse.Offset);

                        var newException = new KafkaApplicationException(
                            "Failed at producing the message with offset {0} for queue {1} in topic {2}",
                            messageResponse.Offset,
                            queueId, _options.TopicName)
                        {
                            ErrorCode = messageResponse.Error,
                            Source = "KafkaStreamProvider"
                        };
                        allResponsesExceptions.Add(newException);
                    }

                    // Aggregating the exceptions, and putting them inside a KafkaStreamProviderException
                    AggregateException exceptions = new AggregateException(allResponsesExceptions);
                    KafkaStreamProviderException exceptionToThrow =
                        new KafkaStreamProviderException("Producing message failed for one or more requests", exceptions);
                    throw exceptionToThrow;
                }
            }
            
            HistogramProducedMessageBatchSize.Update(enumeratedEvents.Count(), queueId.ToString());
            MeterProducedMessagesPerSecond.Mark(queueId.ToString(), 1);
        }
    }
}