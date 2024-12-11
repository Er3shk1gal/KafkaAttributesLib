using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ReflectionExceptions
{
    public class InvokeMethodException : ReflectionException
    {
        public InvokeMethodException(string message) : base(message)
        {
        }
    }
}