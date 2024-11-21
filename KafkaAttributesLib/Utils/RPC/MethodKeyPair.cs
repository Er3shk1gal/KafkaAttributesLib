using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.RPC
{
    public class MethodKeyPair
    {
        public object MessageKey { get; set; } = "";
        public string MessageMethod {get;set;} = "";
    }
}