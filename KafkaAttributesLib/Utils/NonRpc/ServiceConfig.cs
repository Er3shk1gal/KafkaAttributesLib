using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Utils.MessageHandler
{
    public class ServiceConfig
    {
        public string ServiceName { get; set; } = null!;
        public int partition { get; set; }
    }
}