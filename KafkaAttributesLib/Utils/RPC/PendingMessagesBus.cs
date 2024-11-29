using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.RPC
{
    public class PendingMessagesBus
    {
        public RpcTopic TopicInfo {get;set;} = null!;
        public HashSet<MethodKeyPair> MessageKeys {get;set;} = new HashSet<MethodKeyPair>();
    }
}