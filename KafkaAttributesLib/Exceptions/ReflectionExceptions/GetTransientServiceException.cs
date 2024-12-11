using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ReflectionExceptions
{
    public class GetTransientServiceException : ReflectionException
    {
        public GetTransientServiceException(string message) : base(message)
        {
        }
    }
}