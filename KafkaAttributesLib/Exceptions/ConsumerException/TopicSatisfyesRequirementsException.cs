using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.ProducerExceptions
{
    public class TopicSatisfyesRequirementsException : ConsumerException
    {
        public TopicSatisfyesRequirementsException()
        {
        }

        public TopicSatisfyesRequirementsException(string message)
            : base(message)
        {
        }

        public TopicSatisfyesRequirementsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}