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
        public List<RpcTopic> responseTopics { get; set; } = new List<RpcTopic>();
        public MessageSendingVariant messageSendingVariant { get; set; }
        public short replicationFactorStandart { get; set; } = 1;
    }
}