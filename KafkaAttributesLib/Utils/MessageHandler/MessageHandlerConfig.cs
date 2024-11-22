using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaAttributesLib.Utils.MessageHandler
{
    public class MessageHandlerConfig
    {
        public TopicConfig topicConfig {get; set;} = null!;
        public ConsumerConfig consumerConfig {get; set;} = null!;
        public ProducerConfig producerConfig {get; set;} = null!;

    }
}