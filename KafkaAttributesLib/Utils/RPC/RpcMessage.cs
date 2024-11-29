using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RpcMessage<T,Q>
    {
        public Message<T, Q> Message { get; set; } = null!;
        public bool IsPending { get; set; } = false;
    }
}