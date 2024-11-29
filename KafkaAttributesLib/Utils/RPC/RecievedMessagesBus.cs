using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RecievedMessagesBus
    {
        public RpcTopic TopicInfo { get; set; } = null!;
        public HashSet<Message<object,object>> Messages { get; set;} = new HashSet<Message<object,object>>();
    }
}