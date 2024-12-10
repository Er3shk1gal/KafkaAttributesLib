using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.RPC
{
    public class RpcServiceConfig
    {
        public string ServiceName { get; set; } = null!;
        public int RequestPartition { get; set; }
        public int ResponsePartition { get; set; }
    }
}