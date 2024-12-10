using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RpcMessageHandlerConfig
    {
        public RpcTopicPairConfig rpcTopicPairConfig {get; set;} = null!;
        public ConsumerConfig consumerConfig {get; set;} = null!;
        public ProducerConfig producerConfig {get; set;} = null!;
    }
}