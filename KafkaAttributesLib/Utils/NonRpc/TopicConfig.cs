using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.MessageHandler
{
    public class TopicConfig
    {
        public string TopicName { get; set; } = null!;
        public int PartitionCount { get; set; }
        public List<ServiceConfig> Services { get; set; } = null!;
    }
}