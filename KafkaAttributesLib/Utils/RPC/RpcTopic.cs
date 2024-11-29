using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RpcTopic
    {
        public string TopicName { get; set; } = null!;
        public int Partition { get; set; }
    }
}