using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ReflectionExceptions
{
    public class GetScopedServiceException : ReflectionException
    {
        public GetScopedServiceException(string message) : base(message)
        {
        }
    }
}