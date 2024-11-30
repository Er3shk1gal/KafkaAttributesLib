using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaAttributesLib.NonRpc
{
    public class KafkaProducer
    {
        private IProducer<object, object> _producer;
        private readonly ILogger<KafkaProducer> _logger;
        private readonly KafkaTopicManager _kafkaTopicManager;
        public KafkaProducer(IProducer<object,object> producer, ILogger<KafkaProducer> logger, KafkaTopicManager kafkaTopicManager)
    }
}