using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ProducerExceptions
{
   public class ConfigureMessageBusException : MyKafkaException
   {
      public ConfigureMessageBusException() {}
      public ConfigureMessageBusException(string message) : base(message) {}
      public ConfigureMessageBusException(string message, System.Exception inner) : base(message, inner) {}
   }
}