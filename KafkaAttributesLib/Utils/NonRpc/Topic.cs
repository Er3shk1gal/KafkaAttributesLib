using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.NonRpc
{
    public class Topic
    {
        public string TopicName { get; set; } = null!;
        public int Partition { get; set; }
    }
}