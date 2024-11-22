using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ProducerExceptions
{
    public class UnconfiguredServiceMethodsExeption : ConsumerException
    {
        public UnconfiguredServiceMethodsExeption()
        {
        }

        public UnconfiguredServiceMethodsExeption(string message)
            : base(message)
        {
        }

        public UnconfiguredServiceMethodsExeption(string message, Exception innerException)
            : base(message, innerException)
        {
    }
    }
}