using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaAttributesLib.Utils.MessageHandler;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RpcTopicPairConfig
    {
        public string RequestTopicName { get; set; } = null!;
        public int RequestPartitionCount { get; set; }
        public string ResponseTopicName { get; set; } = null!;
        public int ResponsePartitionCount { get; set; } 
        public short ReplicationFactorStandart {get; set;}
        public List<RpcServiceConfig> Services { get; set; } = null!;
    }
}