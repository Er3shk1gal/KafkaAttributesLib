using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RPCConfig
    {
        public ConsumerConfig consumerConfig { get; set; } = null!;
        public ProducerConfig producerConfig { get; set; } = null!;
        public List<string> responseTopics { get; set; } = new List<string>();
        public enum MessageSendingVariant messageSendingVariant { get; set; }
    }
}