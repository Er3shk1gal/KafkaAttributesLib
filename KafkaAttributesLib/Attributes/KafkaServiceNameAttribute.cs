using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class KafkaServiceNameAttribute : Attribute
    {
        public string ServiceName { get; }
        public string ResponseTopicName { get; }
        public int ResponsePartition {get;}

        public KafkaServiceNameAttribute(string serviceName)
        {
            ServiceName = serviceName;
        }
    }
}