using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ReflectionExceptions
{
    public class ReflectionException : Exception
    {
        public ReflectionException()
        {}

        public ReflectionException(string message)
            : base(message)
        {}

        public ReflectionException(string message, Exception innerException)
            : base(message, innerException)
        {}
    }
}