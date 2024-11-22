using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.MessageHandler
{
    public class ServiceMethodPair
    {
        public Type Service { get; set; } = null!;
        public MethodInfo Method { get; set;} = null!;
    }
}